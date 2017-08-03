import _ from 'lodash'
import test from 'tape'
import level from 'levelup'
import memdown from 'memdown'
import Connection from '../src/connection'
import Transactor from '../src/transactor'

test('Ensure the Connection warms up right', async t => {
  try {
    const db = level(memdown)

    function fbStateEquals (fb, txn, userSchema) {
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
        userSchema
      )
    }

    const transactor = await Transactor(db)
    await transactor.transact([
      ['1', '_db/attribute', 'name'],
      ['1', '_db/type', 'String']
    ])
    await transactor.transact([
      ['2', '_db/attribute', 'birthday'],
      ['2', '_db/type', 'String']
    ])
    await transactor.transact([
      ['3', '_db/attribute', 'birthday'],
      ['3', '_db/type', 'Date']
    ])

    const conn = await Connection(db)
    const fb2 = await conn.asOf(2)
    const fb1 = await conn.asOf(1)
    fbStateEquals(fb1, 1, {
      name: {
        '_db/attribute': 'name',
        '_db/type': 'String'
      }
    })
    fbStateEquals(fb2, 2, {
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
  } catch (e) {
    t.end(e)
  }
})
