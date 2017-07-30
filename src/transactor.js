import _ from 'lodash'
import contra from 'contra'
import HashIndex from 'level-hash-index'
import AsyncQ from './any-value-async-q'
import Connection from './connection'
import constants from './constants'
import * as SchemaUtils from './schema-utils'
import toPaddedBase36 from './utils/toPaddedBase36'

function tupleToDBOps (fb, txn, tuple, callback) {
  contra.map(
    [tuple[0], tuple[1], tuple[2]],
    fb.hindex.put,
    (err, hashDatas) => {
      if (err) {
        return callback(err)
      }

      const ops = []
      const fact = {
        t: toPaddedBase36(txn, 6), // for lexo-graphic sorting
        o: tuple[3]
      }
      'eav'.split('').forEach((k, i) => {
        fact[k] = hashDatas[i].hash
        if (hashDatas[i].is_new) {
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
      callback(null, ops)
    }
  )
}

function validateAndEncodeFactTuple (fb, factTuple) {
  // eavo
  if (!_.isArray(factTuple) || factTuple.length < 3 || factTuple.length > 4) {
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

function validateAndEncodeFactTuplesToDBOps (fb, txn, factTuples, callback) {
  try {
    factTuples = validateAndEncodeFactTuples(fb, factTuples)
  } catch (err) {
    return callback(err)
  }

  contra.map(
    factTuples,
    (tuple, callback) => {
      tupleToDBOps(fb, txn, tuple, callback)
    },
    (err, opsPerFact) => {
      callback(err, _.flatten(opsPerFact))
    }
  )
}

function factTuplesToSchemaChanges (conn, txn, factTuples, callback) {
  const attrIds = _.pluck(
    factTuples.filter(fact => fact[1] === '_db/attribute'),
    0
  )

  if (attrIds.length === 0) {
    return callback(null, {})
  }
  conn.loadSchemaFromIds(txn, attrIds, callback)
}

export default function (db, options, onStartup) {
  if (arguments.length === 2) {
    onStartup = options
    options = {}
  }
  options = options || {}
  const hindex = options.hindex || HashIndex(db)

  Connection(db, { hindex }, (err, conn) => {
    if (err) {
      return onStartup(err)
    }

    const q = AsyncQ((data, callback) => {
      const factTuples = data[0]
      const txData = data[1]

      const fb = conn.snap()
      const txn = fb.txn + 1

      // store facts about the transaction
      txData['_db/txn-time'] = new Date()
      _.each(txData, (val, attr) => {
        factTuples.push(['_txid' + txn, attr, val])
      })

      validateAndEncodeFactTuplesToDBOps(fb, txn, factTuples, (err, ops) => {
        if (err) {
          return callback(err)
        }

        fb.db.batch(ops, err => {
          if (err) {
            return callback(err)
          }
          factTuplesToSchemaChanges(
            conn,
            txn,
            factTuples,
            (err, schemaChanges) => {
              if (err) {
                return callback(err)
              }
              conn.update(txn, schemaChanges)
              callback(null, conn.snap())
            }
          )
        })
      })
    })

    onStartup(null, {
      connection: conn,
      transact (factTuples, txData, callback) {
        if (arguments.length === 2) {
          callback = txData
          txData = {}
        }
        q.push([factTuples, txData], callback)
      }
    })
  })
}
