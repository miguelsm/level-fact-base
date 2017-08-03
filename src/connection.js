import _ from 'lodash'
import HashIndex from 'level-hash-index'
import q from './q'
import constants from './constants'
import getEntity from './get-entity'

function getLatestTxn (db) {
  return new Promise((resolve, reject) => {
    const stream = db
      .createReadStream({
        keys: true,
        values: false,
        reverse: true,
        gte: 'teavo!\x00',
        lte: 'teavo!\xFF'
      })
      .on('data', data => {
        resolve(parseInt(data.split('!')[1], 36))
        stream.destroy()
      })
      .on('error', err => {
        reject(err)
      })
      .on('end', () => {
        resolve(0)
      })
  })
}

async function buildSchemaFromEntities (hindex, entities) {
  try {
    const schema = {}
    schema['_db/attribute-hashes'] = {}
    for (const entity of entities) {
      const a = entity['_db/attribute']
      schema[a] = _.cloneDeep(entity)
      const h = await hindex.put(a)
      schema[a]['_db/attribute-hash'] = h.hash
      schema['_db/attribute-hashes'][h.hash] = a
    }
    return schema
  } catch (e) {
    throw e
  }
}

async function loadSchemaFromIds (fb, ids) {
  try {
    const entities = await Promise.all(ids.map(id => getEntity(fb, id)))
    return await buildSchemaFromEntities(fb.hindex, entities)
  } catch (e) {
    throw e
  }
}

async function loadUserSchema (fb) {
  try {
    const results = await q(fb, [['?attr_id', '_db/attribute']], [{}])
    return await loadSchemaFromIds(
      fb,
      results.map(result => result['?attr_id'])
    )
  } catch (e) {
    throw e
  }
}

function makeFB (db, hindex, txn, schema) {
  return {
    db,
    hindex,
    schema,
    txn,
    types: constants.dbTypes
  }
}

async function loadSchemaAsOf (db, hindex, baseSchema, txn) {
  try {
    const userSchema = await loadUserSchema(makeFB(db, hindex, txn, baseSchema))
    return { ...userSchema, ...baseSchema }
  } catch (e) {
    throw e
  }
}

export default async function (db, options) {
  try {
    options = options || {}
    const hindex = options.hindex || HashIndex(db)
    const _makeFB = makeFB.bind(null, db, hindex)
    const baseSchema = await buildSchemaFromEntities(hindex, constants.dbSchema)
    const _loadSchemaAsOf = loadSchemaAsOf.bind(null, db, hindex, baseSchema)
    let latestTxn = await getLatestTxn(db)
    let latestSchema = await _loadSchemaAsOf(latestTxn)
    return {
      update (new_txn, schema_changes) {
        latestTxn = new_txn
        latestSchema = _.assign({}, latestSchema, schema_changes)
      },
      snap () {
        return _makeFB(latestTxn, latestSchema)
      },
      async asOf (txn) {
        try {
          const schema = await _loadSchemaAsOf(txn)
          return _makeFB(txn, schema)
        } catch (e) {
          throw e
        }
      },
      loadSchemaFromIds (txn, ids) {
        return loadSchemaFromIds(_makeFB(txn, baseSchema), ids)
      }
    }
  } catch (e) {
    throw e
  }
}
