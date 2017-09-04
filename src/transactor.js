import _ from 'lodash'
import PQueue from 'p-queue'
import HashIndex from 'level-hash-index'
import Connection from './connection'
import constants from './constants'
import * as SchemaUtils from './schema-utils'
import q from './q'
import toPaddedBase36 from './utils/to-padded-base36'

const TXN_LENGTH = 6

function isMultiValued (fb, a) {
  try {
    return SchemaUtils.isAttributeMultiValued(fb, a)
  } catch (e) {
    return false
  }
}

function getFactTxn (fb, { e, a, v }) {
  const key = `eavto!${e}!${a}!${v}!`
  return new Promise((resolve, reject) => {
    fb.db
      .createKeyStream({
        gte: key + '\x00',
        lte: key + '\xff'
      })
      .on('data', data => {
        resolve(data.substr(key.length, TXN_LENGTH))
      })
      .on('error', err => {
        reject(err)
      })
      .on('end', () => {
        resolve(null)
      })
  })
}

function getDbOps (type, fact) {
  const ops = []
  constants.indexNames.forEach(index => {
    const op = {
      type,
      key: index + '!' + index.split('').map(k => fact[k]).join('!'),
      value: 0
    }
    ops.push(op)
  })
  return ops
}

async function tupleToDBOps (fb, txn, tuple) {
  try {
    const [e, a, v] = tuple
    const hashDatas = await Promise.all([e, a, v].map(fb.hindex.put))
    const ops = []
    const fact = {
      t: toPaddedBase36(txn, TXN_LENGTH), // for lexo-graphic sorting
      o: tuple[3]
    }
    'eav'.split('').forEach((k, i) => {
      fact[k] = hashDatas[i].hash
      if (hashDatas[i].isNew) {
        ops.push({ type: 'put', key: hashDatas[i].key, value: tuple[i] })
      }
    })
    const existingFact = { ...fact, o: 1, t: await getFactTxn(fb, fact) }
    const exists = existingFact.t !== null
    if (!exists && !isMultiValued(fb, a)) {
      const prevV = await q(fb, [[e, a, '?v']])
      if (prevV.length > 0) {
        const prevVHash = await fb.hindex.put(prevV[0]['?v'])
        const prevFact = { ...fact, v: prevVHash.hash }
        ops.push(
          getDbOps('del', { ...prevFact, t: await getFactTxn(fb, prevFact) })
        )
      }
    }
    if (exists && !fact.o) {
      ops.push(getDbOps('del', existingFact))
    }
    if (!exists && fact.o) {
      ops.push(getDbOps('put', fact))
    }
    return _.flatten(ops)
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
    return _.flatten(opsPerFact).filter(t => t)
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
    for (const k of Object.keys(txData)) {
      factTuples.push([`_txid${txn}`, k, txData[k]])
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
