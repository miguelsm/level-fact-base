import _ from 'lodash'
import q from './q'
import assertFB from './utils/assert-fb'
import * as SchemaUtils from './schema-utils'

function isMultiValued (fb, a) {
  try {
    return SchemaUtils.isAttributeMultiValued(fb, a)
  } catch (e) {
    return false
  }
}

export default async function (fb, e) {
  try {
    assertFB(fb)
    const results = await q(fb, [['?e', '?a', '?v']], [{ '?e': e }])
    if (results.length === 0) {
      return null
    }
    const o = {}
    for (const result of results) {
      const a = result['?a']
      if (isMultiValued(fb, a)) {
        if (!_.isArray(o[a])) {
          o[a] = []
        }
        o[a].push(result['?v'])
      } else {
        o[a] = result['?v']
      }
    }
    return o
  } catch (e) {
    throw e
  }
}
