import _ from 'lodash'
import contra from 'contra'
import test from 'tape'
import level from 'levelup'
import memdown from 'memdown'
import HashIndex from 'level-hash-index'
import q from '../src/q'
import Transactor from '../src/transactor'
import genRandomString from '../src/utils/genRandomString'

test('ensure schema is loaded on transactor startup', t => {
  const db = level(memdown)

  Transactor(db, {}, (err, transactor1) => {
    if (err) {
      return t.end(err)
    }
    transactor1.transact([['sky', 'color', 'blue']], {}, err => {
      t.ok(err)
      t.equals(err.toString(), 'Error: Attribute not found: color')

      transactor1.transact(
        [['01', '_db/attribute', 'color'], ['01', '_db/type', 'String']],
        {},
        err => {
          if (err) {
            return t.end(err)
          }
          Transactor(db, {}, (err, transactor2) => {
            if (err) {
              return t.end(err)
            }
            transactor2.transact([['sky', 'color', 'blue']], {}, err => {
              t.end(err)
            })
          })
        }
      )
    })
  })
})

test('ensure schema is updated as facts are recorded', t => {
  const db = level(memdown)

  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return t.end(err)
    }

    transactor.transact([['sky', 'color', 'blue']], {}, err => {
      t.ok(err)
      t.equals(err.toString(), 'Error: Attribute not found: color')

      transactor.transact(
        [['01', '_db/attribute', 'color'], ['01', '_db/type', 'String']],
        {},
        err => {
          if (err) {
            return t.end(err)
          }
          transactor.transact([['sky', 'color', 'blue']], {}, err => {
            t.end(err)
          })
        }
      )
    })
  })
})

test('ensure transact persists stuff to the db', t => {
  const db = level(memdown)

  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return t.end(err)
    }
    contra.series(
      [
        contra.curry(
          transactor.transact,
          [
            ['01', '_db/attribute', 'name'],
            ['01', '_db/type', 'String'],
            ['02', '_db/attribute', 'age'],
            ['02', '_db/type', 'Integer'],
            ['03', '_db/attribute', 'user_id'],
            ['03', '_db/type', 'Entity_ID']
          ],
          {}
        ),
        contra.curry(
          transactor.transact,
          [
            ['0001', 'name', 'bob'],
            ['0001', 'age', 34],
            ['0002', 'name', 'jim'],
            ['0002', 'age', 23]
          ],
          { user_id: '0001' }
        )
      ],
      err => {
        if (err) {
          return t.end(err)
        }
        const all_data = []
        db
          .readStream()
          .on('data', data => {
            all_data.push(data)
          })
          .on('close', () => {
            t.equals(all_data.length, 74)
            t.end()
          })
      }
    )
  })
})

test('ensure transactor warms up with the latest transaction id', t => {
  const db = level(memdown)

  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return t.end(err)
    }
    contra.series(
      [
        contra.curry(
          transactor.transact,
          [['01', '_db/attribute', 'is'], ['01', '_db/type', 'String']],
          {}
        ),
        contra.curry(transactor.transact, [['bob', 'is', 'cool']], {}),
        contra.curry(transactor.transact, [['bob', 'is', 'NOT cool']], {}),
        contra.curry(transactor.transact, [['bob', 'is', 'cool']], {})
      ],
      err => {
        if (err) {
          return t.end(err)
        }
        const fb = transactor.connection.snap()
        q(fb, [['?_', '?_', '?_', '?txn']], [{}], (err, results) => {
          if (err) {
            return t.end(err)
          }
          const txns = _.unique(_.pluck(results, '?txn')).sort()
          t.deepEqual(txns, [1, 2, 3, 4])

          //warm up a new transactor to see where it picks up
          Transactor(db, {}, (err, transactor2) => {
            if (err) {
              return t.end(err)
            }
            transactor2.transact(
              [['bob', 'is', 'NOT cool']],
              {},
              (err, fb2) => {
                if (err) {
                  return t.end(err)
                }
                q(fb2, [['?_', '?_', '?_', '?txn']], [{}], (err, results) => {
                  const txns = _.unique(_.pluck(results, '?txn')).sort()
                  t.deepEqual(txns, [1, 2, 3, 4, 5])
                  t.end(err)
                })
              }
            )
          })
        })
      }
    )
  })
})

test('transactions must be done serially, in the order they are recieved', t => {
  const db = level(memdown)
  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return t.end(err)
    }
    function transact_attr (attr_name) {
      return function (callback) {
        transactor.transact(
          [['01', '_db/attribute', attr_name], ['01', '_db/type', 'String']],
          {},
          (err, fb) => {
            callback(null, err ? 'fail' : fb.txn)
          }
        )
      }
    }
    contra.concurrent(
      [
        transact_attr('works'),
        transact_attr(111), //fails
        transact_attr('also works')
      ],
      (err, results) => {
        if (err) {
          return t.end(err)
        }
        t.deepEquals(results, [1, 'fail', 2])
        t.end()
      }
    )
  })
})

function setUpRetractTest (multiValued, callback) {
  const db = level(memdown)
  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return callback(err)
    }
    contra.series(
      [
        contra.curry(
          transactor.transact,
          [
            ['1', '_db/attribute', 'email'],
            ['1', '_db/type', 'String'],
            ['1', '_db/is-multi-valued', multiValued]
          ],
          {}
        ),
        contra.curry(transactor.transact, [['bob', 'email', 'email@1']], {}),
        contra.curry(transactor.transact, [['bob', 'email', 'email@2']], {}),
        contra.curry(
          transactor.transact,
          [['bob', 'email', 'email@2', false]],
          {}
        ),
        contra.curry(transactor.transact, [['bob', 'email', 'email@3']], {}),
        contra.curry(transactor.transact, [['bob', 'email', 'email@2']], {}),
        contra.curry(
          transactor.transact,
          [['bob', 'email', 'email@1', false]],
          {}
        ),
        contra.curry(
          transactor.transact,
          [['bob', 'email', 'email@2', false]],
          {}
        ),
        contra.curry(
          transactor.transact,
          [['bob', 'email', 'email@3', false]],
          {}
        )
      ],
      (err, fbs) => {
        if (err) {
          return callback(err)
        }
        contra.map.series(
          fbs,
          (fb, callback) => {
            q(fb, [['bob', 'email', '?email']], [{}], (err, results) => {
              callback(err, _.pluck(results, '?email').sort())
            })
          },
          callback
        )
      }
    )
  })
}

test('retracting facts', t => {
  setUpRetractTest(false, (err, emails_over_time) => {
    if (err) {
      return t.end(err)
    }
    t.deepEquals(emails_over_time, [
      [],
      ['email@1'],
      ['email@2'],
      [],
      ['email@3'],
      ['email@2'],
      [],
      [],
      []
    ])
    t.end()
  })
})

test('retracting multi-valued facts', t => {
  setUpRetractTest(true, (err, emails_over_time) => {
    if (err) {
      return t.end(err)
    }
    t.deepEquals(emails_over_time, [
      [],
      ['email@1'],
      ['email@1', 'email@2'],
      ['email@1'],
      ['email@1', 'email@3'],
      ['email@1', 'email@2', 'email@3'],
      ['email@2', 'email@3'],
      ['email@3'],
      []
    ])
    t.end()
  })
})
