import _ from 'lodash'
import PQueue from 'p-queue'
import HashIndex from 'level-hash-index'
import Connection from './connection'
import constants from './constants'
import * as SchemaUtils from './schema-utils'
import toPaddedBase36 from './utils/to-padded-base36'

async function tupleToDBOps (fb, txn, tuple) {
  try {
    const [e, a, v] = tuple
    const hashDatas = await Promise.all([e, a, v].map(fb.hindex.put))
    const ops = []
    const fact = {
      t: toPaddedBase36(txn, 6), // for lexo-graphic sorting
      o: tuple[3]
    }
    'eav'.split('').forEach((k, i) => {
      fact[k] = hashDatas[i].hash
      if (hashDatas[i].isNew) {
        ops.push({ type: 'put', key: hashDatas[i].key, value: tuple[i] })
      }
    })
    constants.indexNames.forEach(index => {
      ops.push({
        type: 'put',
        key: index + '!' + index.split('').map(k => fact[k]).join('!'),
        value: 0
      })
    })
    return ops
  } catch (e) {
    throw e
  }
}

function validateAndEncodeFactTuple (fb, factTuple) {
  // eavo
  if (
    !Array.isArray(factTuple) ||
    factTuple.length < 3 ||
    factTuple.length > 4
  ) {
    throw new Error('factTuple must be an array defining EAV or EAVO')
  }

  // entity
  let e = factTuple[0]
  if (!fb.types['Entity_ID'].validate(e)) {
    throw new Error('Not a valid entity id')
  }
  e = fb.types['Entity_ID'].encode(e)

  // attribute
  const a = factTuple[1]
  const type = SchemaUtils.getTypeForAttribute(fb, a)

  // value
  let v = factTuple[2]
  if (!type.validate(v)) {
    throw new Error('Invalid value for attribute ' + a)
  }
  v = type.encode(v)

  // op
  const o = factTuple[3] === false ? 0 : 1 // default to 1

  return [e, a, v, o]
}

function validateAndEncodeFactTuples (fb, factTuples) {
  return factTuples.map(tuple => validateAndEncodeFactTuple(fb, tuple))
}

async function validateAndEncodeFactTuplesToDBOps (fb, txn, factTuples) {
  try {
    factTuples = validateAndEncodeFactTuples(fb, factTuples)
    const opsPerFact = await Promise.all(
      factTuples.map(tuple => tupleToDBOps(fb, txn, tuple))
    )
    return _.flatten(opsPerFact)
  } catch (e) {
    throw e
  }
}

async function factTuplesToSchemaChanges (conn, txn, factTuples) {
  try {
    const attrIds = factTuples.reduce((acc, fact) => {
      if (fact[1] === '_db/attribute') {
        acc.push(fact[0])
      }
      return acc
    }, [])
    if (attrIds.length === 0) {
      return {}
    }
    return conn.loadSchemaFromIds(txn, attrIds)
  } catch (e) {
    throw e
  }
}

async function worker (conn, data) {
  try {
    const factTuples = data[0]
    const txData = data[1]
    const fb = conn.snap()
    const txn = fb.txn + 1

    // store facts about the transaction
    txData['_db/txn-time'] = new Date()
    for (const k of Object.keys(txData)) {
      const val = txData[k]
      factTuples.push(['_txid' + txn, k, val])
    }

    const ops = await validateAndEncodeFactTuplesToDBOps(fb, txn, factTuples)
    return await new Promise((resolve, reject) => {
      fb.db.batch(ops, async err => {
        try {
          if (err) {
            reject(err)
          }
          const schemaChanges = await factTuplesToSchemaChanges(
            conn,
            txn,
            factTuples
          )
          conn.update(txn, schemaChanges)
          resolve(conn.snap())
        } catch (e) {
          reject(e)
        }
      })
    })
  } catch (e) {
    throw e
  }
}

export default async function (db, options = {}) {
  try {
    const hindex = options.hindex || HashIndex(db)
    const conn = await Connection(db, { hindex })
    const queue = new PQueue({ concurrency: 1 })
    return {
      connection: conn,
      transact (factTuples, txData = {}) {
        return queue.add(() => worker(conn, [factTuples, txData]))
      }
    }
  } catch (e) {
    throw e
  }
}
