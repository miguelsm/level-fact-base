import contra from 'contra'
import test from 'tape'
import level from 'levelup'
import memdown from 'memdown'
import getEntity from '../src/getEntity'
import Transactor from '../src/transactor'

test('getEntity', t => {
  const db = level(memdown)
  Transactor(db, (err, transactor) => {
    if (err) {
      return t.end(err)
    }
    contra.series(
      [
        contra.curry(transactor.transact, [
          ['01', '_db/attribute', 'email'],
          ['01', '_db/type', 'String'],
          ['02', '_db/attribute', 'name'],
          ['02', '_db/type', 'String']
        ]),

        contra.curry(transactor.transact, [
          ['u0', 'email', 'andy@email.com'],
          ['u0', 'name', 'andy']
        ]),

        contra.curry(transactor.transact, [
          ['u1', 'email', 'opie@email.com'],
          ['u1', 'name', 'opie']
        ]),

        contra.curry(transactor.transact, [['u0', 'email', 'new@email.com']])
      ],
      err => {
        if (err) {
          return t.end(err)
        }
        const fb = transactor.connection.snap()
        contra.concurrent(
          {
            u0: contra.curry(getEntity, fb, 'u0'),
            u1: contra.curry(getEntity, fb, 'u1'),
            u2: contra.curry(getEntity, fb, 'u2')
          },
          (err, r) => {
            t.deepEqual(r.u0, { name: 'andy', email: 'new@email.com' })
            t.deepEqual(r.u1, { name: 'opie', email: 'opie@email.com' })
            t.deepEqual(r.u2, null)
            t.end(err)
          }
        )
      }
    )
  })
})
