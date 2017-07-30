import _ from 'lodash'
import contra from 'contra'
import HashIndex from 'level-hash-index'
import q from './q'
import constants from './constants'
import getEntity from './getEntity'

function getLatestedTxn (db, callback) {
  const stream = db
    .createReadStream({
      keys: true,
      values: false,
      reverse: true,
      gte: 'teavo!\x00',
      lte: 'teavo!\xFF'
    })
    .on('data', data => {
      const txn = parseInt(data.split('!')[1], 36)
      callback(null, txn)
      stream.destroy()
    })
    .on('error', err => {
      callback(err)
    })
    .on('end', () => {
      callback(null, 0)
    })
}

function buildSchemaFromEntities (hindex, entities, callback) {
  const schema = {}
  schema['_db/attribute-hashes'] = {}

  contra.each(
    entities,
    (entity, done) => {
      const a = entity['_db/attribute']
      schema[a] = _.cloneDeep(entity)

      hindex.put(a, (err, h) => {
        if (err) {
          return done(err)
        }
        schema[a]['_db/attribute-hash'] = h.hash
        schema['_db/attribute-hashes'][h.hash] = a
        done(null)
      })
    },
    err => {
      if (err) {
        return callback(err)
      }
      callback(null, schema)
    }
  )
}

function loadSchemaFromIds (fb, ids, callback) {
  contra.map(
    ids,
    (id, callback) => {
      getEntity(fb, id, callback)
    },
    (err, entities) => {
      if (err) {
        return callback(err)
      }
      buildSchemaFromEntities(fb.hindex, entities, callback)
    }
  )
}

function loadUserSchema (fb, callback) {
  q(fb, [['?attr_id', '_db/attribute']], [{}], (err, results) => {
    if (err) {
      return callback(err)
    }
    loadSchemaFromIds(
      fb,
      results.map(result => {
        return result['?attr_id']
      }),
      callback
    )
  })
}

export default function (db, options, callback) {
  if (arguments.length === 2) {
    callback = options
    options = {}
  }

  options = options || {}
  const hindex = options.hindex || HashIndex(db)

  function makeFB (txn, schema) {
    return {
      db,
      hindex,
      schema,
      txn,
      types: constants.dbTypes
    }
  }

  buildSchemaFromEntities(hindex, constants.dbSchema, (err, base_schema) => {
    if (err) {
      return callback(err)
    }

    function loadSchemaAsOf (txn, callback) {
      loadUserSchema(makeFB(txn, base_schema), (err, user_schema) => {
        if (err) {
          return callback(err)
        }
        callback(null, _.assign({}, user_schema, base_schema))
      })
    }

    getLatestedTxn(db, (err, latest_transaction_n) => {
      if (err) {
        return callback(err)
      }

      loadSchemaAsOf(latest_transaction_n, (err, latest_schema) => {
        if (err) {
          return callback(err)
        }

        callback(null, {
          update (new_txn, schema_changes) {
            latest_transaction_n = new_txn
            latest_schema = _.assign({}, latest_schema, schema_changes)
          },
          snap () {
            return makeFB(latest_transaction_n, latest_schema)
          },
          asOf (txn, callback) {
            loadSchemaAsOf(txn, (err, schema) => {
              if (err) {
                return callback(err)
              }
              callback(null, makeFB(txn, schema))
            })
          },
          loadSchemaFromIds (txn, ids, callback) {
            loadSchemaFromIds(makeFB(txn, base_schema), ids, callback)
          }
        })
      })
    })
  })
}
