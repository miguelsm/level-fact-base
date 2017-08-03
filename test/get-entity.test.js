import test from 'tape'
import level from 'levelup'
import memdown from 'memdown'
import getEntity from '../src/get-entity'
import Transactor from '../src/transactor'

test('getEntity', async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    await transactor.transact([
      ['01', '_db/attribute', 'email'],
      ['01', '_db/type', 'String'],
      ['02', '_db/attribute', 'name'],
      ['02', '_db/type', 'String']
    ])
    await transactor.transact([
      ['u0', 'email', 'andy@email.com'],
      ['u0', 'name', 'andy']
    ])
    await transactor.transact([
      ['u1', 'email', 'opie@email.com'],
      ['u1', 'name', 'opie']
    ])
    await transactor.transact([['u0', 'email', 'new@email.com']])
    const fb = transactor.connection.snap()
    const [u0, u1, u2] = await Promise.all([
      getEntity(fb, 'u0'),
      getEntity(fb, 'u1'),
      getEntity(fb, 'u2')
    ])
    t.deepEqual(u0, { name: 'andy', email: 'new@email.com' })
    t.deepEqual(u1, { name: 'opie', email: 'opie@email.com' })
    t.deepEqual(u2, null)
    t.end()
  } catch (e) {
    t.end(e)
  }
})
