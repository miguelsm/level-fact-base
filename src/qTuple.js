import _ from 'lodash'
import contra from 'contra'
import escapeRegExp from 'escape-regexp'
import assertFB from './utils/assertFB'
import * as SchemaUtils from './schema-utils'
import toPaddedBase36 from './utils/toPaddedBase36'

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

function parseElementThroughHIndex (fb, elm, callback) {
  elm = unEscapeVar(elm)
  fb.hindex.getHash(elm, (err, hash) => {
    if (err) {
      callback(err)
    } else {
      callback(null, { hash })
    }
  })
}

function getHashForEachType (fb, elm, callback) {
  const hashByTypeName = {}
  contra.each(
    Object.keys(fb.types),
    (typeName, next) => {
      const type = fb.types[typeName]
      if (!type.validate(elm)) {
        return next(null) // just ignore it b/c elm must not be of that type
      }
      parseElementThroughHIndex(fb, type.encode(elm), (err, o) => {
        if (err) {
          return next(err)
        }
        hashByTypeName[typeName] = o.hash
        next(null)
      })
    },
    err => {
      callback(err, hashByTypeName)
    }
  )
}

function parseElement (fb, tuple, i, callback) {
  const elm = tuple.length < i + 1 ? '?_' : tuple[i]
  if (isTheThrowAwayVar(elm)) {
    callback(null, { isBlank: true })
  } else if (isVar(elm)) {
    callback(null, { varName: elm })
  } else if (i < 2 && _.isString(elm)) {
    parseElementThroughHIndex(fb, elm, callback)
  } else if (i === 2) {
    var type = getTypeForAttribute(fb, tuple[1])
    if (!type) {
      getHashForEachType(fb, elm, (err, typeNotYetKnown) => {
        if (err) {
          return callback(err)
        }
        switch (_.size(typeNotYetKnown)) {
          case 0:
            callback(
              new Error('value in this query tuple is of an unkown type')
            )
            break
          case 1:
            callback(null, { hash: _.first(_.values(typeNotYetKnown)) })
            break
          default:
            callback(null, { typeNotYetKnown })
        }
      })
    } else {
      if (type.validate(elm)) {
        parseElementThroughHIndex(fb, type.encode(elm), callback)
      } else {
        callback(new Error('value in tuple has invalid type'))
      }
    }
  } else if (i === 3 && _.isNumber(elm)) {
    const txn = toPaddedBase36(elm, 6)
    callback(null, { hash: txn })
  } else if (i === 4 && (elm === true || elm === false)) {
    callback(null, { hash: elm })
  } else {
    callback(new Error(`element ${i} in tuple has invalid type`))
  }
}

function parseTuple (fb, tuple, callback) {
  contra.concurrent(
    {
      e: contra.curry(parseElement, fb, tuple, 0),
      a: contra.curry(parseElement, fb, tuple, 1),
      v: contra.curry(parseElement, fb, tuple, 2),
      t: contra.curry(parseElement, fb, tuple, 3),
      o: contra.curry(parseElement, fb, tuple, 4)
    },
    callback
  )
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

function forEachMatchingHashFact (fb, matcher, iterator, done) {
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
      done(err)
    })
    .on('end', () => {
      done(null)
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

export default function (fb, tuple, orig_binding, callback) {
  if (arguments.length === 3) {
    callback = orig_binding
    orig_binding = {}
  }

  try {
    assertFB(fb)
  } catch (e) {
    return callback(e)
  }

  if (!_.isArray(tuple)) {
    return callback(new Error('tuple must be an array'))
  }

  if (!_.isPlainObject(orig_binding)) {
    return callback(new Error('binding must be a plain object'))
  }

  parseTuple(fb, bindToTuple(tuple, orig_binding), (err, qFact) => {
    if (err) {
      if (err.notFound) {
        // one of the tuple values were not found in the hash, so there must be no results
        return callback(null, [])
      }
      return callback(err)
    }

    const indexToUse = selectIndex(qFact)
    const isAttributeUnknown = qFact.a.hasOwnProperty('varName')

    const s = SetOfBindings(fb, qFact)

    forEachMatchingHashFact(
      fb,
      toMatcher(indexToUse, qFact),
      hashFact => {
        s.add(hashFact)
      },
      err => {
        if (err) {
          return callback(err)
        }

        const hashBindings = s.toArray()

        // de-hash the bindings
        contra.map(
          hashBindings,
          (binding, callback) => {
            contra.map(
              _.pairs(binding),
              (p, callback) => {
                let [varName, varValue] = p
                let decode = _.identity

                if (varValue && varValue.decode) {
                  decode = varValue.decode
                  varValue = varValue.hash
                }

                if (_.isString(varValue)) {
                  fb.hindex.get(varValue, (err, val) => {
                    callback(err, [varName, decode(val)])
                  })
                } else {
                  callback(null, [varName, varValue])
                }
              },
              (err, pairs) => {
                callback(err, _.assign({}, orig_binding, _.object(pairs)))
              }
            )
          },
          callback
        )
      }
    )
  })
}
