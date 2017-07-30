import _ from 'lodash'
import q from './q'
import assertFB from './utils/assertFB'
import * as SchemaUtils from './schema-utils'

function isMultiValued (fb, a) {
  try {
    return SchemaUtils.isAttributeMultiValued(fb, a)
  } catch (e) {
    return false
  }
}

export default function (fb, e, callback) {
  try {
    assertFB(fb)
  } catch (e) {
    return callback(e)
  }

  q(fb, [['?e', '?a', '?v']], [{ '?e': e }], (err, results) => {
    if (err) {
      return callback(err)
    }
    if (results.length === 0) {
      return callback(null, null)
    }
    const o = {}
    results.forEach(result => {
      const a = result['?a']
      if (isMultiValued(fb, a)) {
        if (!_.isArray(o[a])) {
          o[a] = []
        }
        o[a].push(result['?v'])
      } else {
        o[a] = result['?v']
      }
    })
    callback(null, o)
  })
}
