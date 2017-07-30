import _ from 'lodash'
import contra from 'contra'
import qTuple from './qTuple'
import assertFB from './utils/assertFB'

export default function (fb, tuples, bindings, callback) {
  if (arguments.length === 3) {
    callback = bindings
    bindings = [{}]
  }

  try {
    assertFB(fb)
  } catch (e) {
    return callback(e)
  }

  if (!_.isArray(tuples)) {
    return callback(new Error('q expects an array of tuples'))
  }

  if (!_.isArray(bindings)) {
    return callback(new Error('q expects an array bindings'))
  }

  let memo = bindings

  contra.each.series(
    tuples,
    (tuple, callback) => {
      contra.map(
        memo,
        (binding, callback) => {
          qTuple(fb, tuple, binding, callback)
        },
        (err, next_bindings) => {
          if (err) {
            return callback(err)
          }
          memo = _.flatten(next_bindings)
          callback()
        }
      )
    },
    err => {
      if (err) callback(err)
      else callback(null, memo)
    }
  )
}
