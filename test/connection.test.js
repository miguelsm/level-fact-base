import _ from 'lodash'
import contra from 'contra'
import test from 'tape'
import level from 'levelup'
import memdown from 'memdown'
import Connection from '../src/connection'
import Transactor from '../src/transactor'

test('Ensure the Connection warms up right', t => {
  const db = level(memdown)

  function fbStateEquals (fb, txn, user_schema) {
    t.equals(fb.txn, txn)
    t.deepEquals(
      _.object(
        _.map(_.filter(_.pairs(fb.schema), p => p[0][0] !== '_'), p => [
          p[0],
          _.object(
            _.filter(_.pairs(p[1]), p1 => p1[0] !== '_db/attribute-hash')
          )
        ])
      ),
      user_schema
    )
  }

  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return t.end(err)
    }

    contra.series(
      [
        contra.curry(
          transactor.transact,
          [['1', '_db/attribute', 'name'], ['1', '_db/type', 'String']],
          {}
        ),
        contra.curry(
          transactor.transact,
          [['2', '_db/attribute', 'birthday'], ['2', '_db/type', 'String']],
          {}
        ),
        contra.curry(
          transactor.transact,
          [['3', '_db/attribute', 'birthday'], ['3', '_db/type', 'Date']],
          {}
        )
      ],
      err => {
        if (err) {
          return t.end(err)
        }

        Connection(db, {}, (err, conn) => {
          if (err) {
            return t.end(err)
          }

          conn.asOf(2, (err, fb_2) => {
            if (err) {
              return t.end(err)
            }
            conn.asOf(1, (err, fb_1) => {
              if (err) {
                return t.end(err)
              }

              fbStateEquals(fb_1, 1, {
                name: {
                  '_db/attribute': 'name',
                  '_db/type': 'String'
                }
              })

              fbStateEquals(fb_2, 2, {
                name: {
                  '_db/attribute': 'name',
                  '_db/type': 'String'
                },
                birthday: {
                  '_db/attribute': 'birthday',
                  '_db/type': 'String'
                }
              })

              fbStateEquals(conn.snap(), 3, {
                name: {
                  '_db/attribute': 'name',
                  '_db/type': 'String'
                },
                birthday: {
                  '_db/attribute': 'birthday',
                  '_db/type': 'Date'
                }
              })
              t.end()
            })
          })
        })
      }
    )
  })
})
