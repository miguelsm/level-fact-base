import _ from 'lodash'
import qTuple from './q-tuple'
import assertFB from './utils/assert-fb'

export default async function (fb, tuples, bindings = [{}]) {
  try {
    assertFB(fb)
    if (!_.isArray(tuples)) {
      throw new Error('q expects an array of tuples')
    }
    if (!_.isArray(bindings)) {
      throw new Error('q expects an array bindings')
    }
    let memo = bindings
    for (const tuple of tuples) {
      const nextBindings = await Promise.all(
        memo.map(binding => qTuple(fb, tuple, binding))
      )
      memo = _.flatten(nextBindings)
    }
    return memo
  } catch (e) {
    throw e
  }
}
