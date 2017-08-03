import _ from 'lodash'
import escapeRegExp from 'escape-regexp'
import assertFB from './utils/assert-fb'
import * as SchemaUtils from './schema-utils'
import toPaddedBase36 from './utils/to-padded-base36'

function escapeVar (elm) {
  return _.isString(elm)
    ? elm.replace(/^\\/, '\\\\').replace(/^\?/, '\\?')
    : elm
}

function unEscapeVar (elm) {
  return _.isString(elm) ? elm.replace(/^\\/, '') : elm
}

function isVar (elm) {
  return _.isString(elm) && elm[0] === '?'
}

function isTheThrowAwayVar (elm) {
  return elm === '?_'
}

function bindToTuple (tuple, binding) {
  return tuple.map(e => (binding.hasOwnProperty(e) ? escapeVar(binding[e]) : e))
}

async function parseElementThroughHIndex (fb, elm) {
  try {
    elm = unEscapeVar(elm)
    const hash = await fb.hindex.getHash(elm)
    return { hash }
  } catch (e) {
    throw e
  }
}

async function getHashForEachType (fb, elm) {
  try {
    const hashByTypeName = {}
    for (const typeName of Object.keys(fb.types)) {
      const type = fb.types[typeName]
      if (!type.validate(elm)) {
        continue // just ignore it b/c elm must not be of that type
      }
      const o = await parseElementThroughHIndex(fb, type.encode(elm))
      hashByTypeName[typeName] = o.hash
    }
    return hashByTypeName
  } catch (e) {
    throw e
  }
}

async function parseElement (fb, tuple, i) {
  try {
    const elm = tuple.length < i + 1 ? '?_' : tuple[i]
    if (isTheThrowAwayVar(elm)) {
      return { isBlank: true }
    } else if (isVar(elm)) {
      return { varName: elm }
    } else if (i < 2 && _.isString(elm)) {
      return await parseElementThroughHIndex(fb, elm)
    } else if (i === 2) {
      var type = getTypeForAttribute(fb, tuple[1])
      if (!type) {
        const typeNotYetKnown = await getHashForEachType(fb, elm)
        switch (_.size(typeNotYetKnown)) {
          case 0:
            throw new Error('value in this query tuple is of an unkown type')
            break
          case 1:
            return { hash: _.first(_.values(typeNotYetKnown)) }
            break
          default:
            return { typeNotYetKnown }
        }
      } else {
        if (type.validate(elm)) {
          return await parseElementThroughHIndex(fb, type.encode(elm))
        } else {
          throw new Error('value in tuple has invalid type')
        }
      }
    } else if (i === 3 && _.isNumber(elm)) {
      const txn = toPaddedBase36(elm, 6)
      return { hash: txn }
    } else if (i === 4 && (elm === true || elm === false)) {
      return { hash: elm }
    } else {
      throw new Error(`element ${i} in tuple has invalid type`)
    }
  } catch (e) {
    throw e
  }
}

async function parseTuple (fb, tuple) {
  try {
    const [e, a, v, t, o] = await Promise.all([
      parseElement(fb, tuple, 0),
      parseElement(fb, tuple, 1),
      parseElement(fb, tuple, 2),
      parseElement(fb, tuple, 3),
      parseElement(fb, tuple, 4)
    ])
    return { e, a, v, t, o }
  } catch (e) {
    throw e
  }
}

const selectIndex = (function () {
  function getKnowns (qFact) {
    let knowns = ''
    'eavt'.split('').forEach(key => {
      knowns += qFact[key].hasOwnProperty('hash') ? key : '_'
    })
    return knowns
  }
  const mapping = {
    ____: 'eavto',

    e___: 'eavto',
    ea__: 'eavto',
    e_v_: 'eavto',
    eav_: 'eavto',

    _a__: 'aveto',
    _av_: 'aveto',

    __v_: 'vaeto',

    ___t: 'teavo',
    e__t: 'teavo',
    ea_t: 'teavo',
    e_vt: 'teavo',
    eavt: 'teavo',
    _a_t: 'teavo',
    _avt: 'teavo',
    __vt: 'teavo'
  }
  return qFact => mapping[getKnowns(qFact)]
})()

function toMatcher (indexToUse, qFact) {
  const prefix = indexToUse + '!'
  const prefixParts = []
  let foundAGap = false

  const keyRegex = new RegExp(
    escapeRegExp(prefix) +
      indexToUse
        .split('')
        .map(k => {
          if (qFact[k].hasOwnProperty('hash')) {
            if (!foundAGap) {
              prefixParts.push(qFact[k].hash)
            }
            return escapeRegExp(qFact[k].hash)
          } else {
            foundAGap = true
            return '.*'
          }
        })
        .join(escapeRegExp('!'))
  )

  return {
    prefix: prefix + prefixParts.join('!'),
    getHashFactIfKeyMatches (fb, key) {
      if (!keyRegex.test(key)) {
        return false
      }
      const hashFact = parseKey(key)
      if (hashFact.t > fb.txn) {
        return false // this fact is too new, so ignore it
      }
      if (qFact.v.hasOwnProperty('typeNotYetKnown')) {
        const typeName = getTypeNameForHash(fb, hashFact.a)
        if (!qFact.v.typeNotYetKnown.hasOwnProperty(typeName)) {
          return false // just ignore this fact b/c types don't line up
        }
        if (qFact.v.typeNotYetKnown[typeName] !== hashFact.v) {
          return false // just ignore this fact b/c it's not the value the user specified
        }
      }
      return hashFact
    }
  }
}

function parseKey (key) {
  const parts = key.split('!')
  const indexName = parts[0]
  const hashFact = {}
  indexName.split('').forEach((k, i) => {
    const part = parts[i + 1]
    if (k === 't') {
      hashFact[k] = parseInt(part, 36)
    } else if (k === 'o') {
      hashFact[k] = part === '1'
    } else {
      hashFact[k] = part
    }
  })
  return hashFact
}

function forEachMatchingHashFact (fb, matcher, iterator) {
  return new Promise((resolve, reject) => {
    fb.db
      .createReadStream({
        keys: true,
        values: false,
        gte: matcher.prefix + '\x00',
        lte: matcher.prefix + '\xFF'
      })
      .on('data', key => {
        const hashFact = matcher.getHashFactIfKeyMatches(fb, key)
        if (!hashFact) {
          return // just ignore and keep going
        }
        iterator(hashFact)
      })
      .on('error', err => {
        reject(err)
      })
      .on('end', () => {
        resolve()
      })
  })
}

function isHashMultiValued (fb, h) {
  try {
    return SchemaUtils.isAttributeHashMultiValued(fb, h)
  } catch (e) {
    return false
  }
}

function getTypeForAttribute (fb, a) {
  try {
    return SchemaUtils.getTypeForAttribute(fb, a)
  } catch (e) {
    return null
  }
}

function getTypeForHash (fb, h) {
  try {
    const a = SchemaUtils.getAttributeFromHash(fb, h)
    return getTypeForAttribute(fb, a)
  } catch (e) {
    return null
  }
}

function getTypeNameForHash (fb, h) {
  try {
    const a = SchemaUtils.getAttributeFromHash(fb, h)
    return SchemaUtils.getTypeNameForAttribute(fb, a)
  } catch (e) {
    return null
  }
}

function SetOfBindings (fb, qFact) {
  let onlyTheLatest = isHashMultiValued(fb, qFact.a.hash)
    ? false
    : qFact.t.isBlank
  const isAttributeUnknown =
    qFact.a.hasOwnProperty('varName') || qFact.a.hasOwnProperty('isBlank')
  const varNames = 'eavto'
    .split('')
    .filter(k => qFact[k].hasOwnProperty('varName'))
    .map(k => [qFact[k].varName, k])
  const set = {}
  const latestFor = {}

  return {
    add (hashFact) {
      if (onlyTheLatest && isAttributeUnknown) {
        onlyTheLatest = !isHashMultiValued(fb, hashFact.a)
      }

      const type = getTypeForHash(
        fb,
        isAttributeUnknown ? hashFact.a : qFact.a.hash
      )
      const keyForLatestFor =
        hashFact.e + hashFact.a + (onlyTheLatest ? '' : hashFact.v)

      if (
        latestFor.hasOwnProperty(keyForLatestFor) &&
        latestFor[keyForLatestFor].txn > hashFact.t
      ) {
        return // not the latest, so skip the rest
      }
      const binding = {}
      let hashKey = '' // to ensure uniqueness
      varNames.forEach(p => {
        var k = p[1]
        if (k === 'v') {
          binding[p[0]] = {
            hash: hashFact[k],
            decode: type.decode
          }
        } else {
          binding[p[0]] = hashFact[k]
        }
        hashKey += hashFact[k]
      })
      set[hashKey] = binding
      latestFor[keyForLatestFor] = {
        hashKey,
        op: hashFact.o,
        txn: hashFact.t
      }
    },
    toArray () {
      const isTheOpAVar = qFact.o.hasOwnProperty('varName')
      return _.unique(
        _.pluck(
          _.filter(latestFor, d => {
            return isTheOpAVar ? true : d.op // remove retractions
          }),
          'hashKey'
        )
      ).map(key => set[key])
    }
  }
}

export default async function (fb, tuple, origBinding = {}) {
  try {
    assertFB(fb)

    if (!_.isArray(tuple)) {
      throw new Error('tuple must be an array')
    }

    if (!_.isPlainObject(origBinding)) {
      throw new Error('binding must be a plain object')
    }

    let qFact

    try {
      qFact = await parseTuple(fb, bindToTuple(tuple, origBinding))
    } catch (e) {
      if (e.notFound) {
        // one of the tuple values were not found in the hash, so there must be no results
        return []
      }
      throw e
    }

    const indexToUse = selectIndex(qFact)

    const s = SetOfBindings(fb, qFact)
    await forEachMatchingHashFact(
      fb,
      toMatcher(indexToUse, qFact),
      hashFact => {
        s.add(hashFact)
      }
    )
    const hashBindings = s.toArray()
    // de-hash the bindings
    const results = await Promise.all(
      hashBindings.map(async binding => {
        try {
          const pairs = await Promise.all(
            _.pairs(binding).map(async p => {
              try {
                let [varName, varValue] = p
                let decode = _.identity

                if (varValue && varValue.decode) {
                  decode = varValue.decode
                  varValue = varValue.hash
                }

                if (_.isString(varValue)) {
                  const val = await fb.hindex.get(varValue)
                  return [varName, decode(val)]
                } else {
                  return [varName, varValue]
                }
              } catch (e) {
                throw e
              }
            })
          )
          return { ...origBinding, ..._.object(pairs) }
        } catch (e) {
          throw e
        }
      })
    )
    return results
  } catch (e) {
    throw e
  }
}
