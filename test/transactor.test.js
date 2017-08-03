import _ from 'lodash'
import test from 'tape'
import level from 'levelup'
import memdown from 'memdown'
import q from '../src/q'
import Transactor from '../src/transactor'

test('ensure schema is loaded on transactor startup', async t => {
  try {
    const db = level(memdown)
    const transactor1 = await Transactor(db)
    try {
      await transactor1.transact([['sky', 'color', 'blue']])
    } catch (e) {
      t.ok(e)
      t.equals(e.toString(), 'Error: Attribute not found: color')
    }
    await transactor1.transact([
      ['01', '_db/attribute', 'color'],
      ['01', '_db/type', 'String']
    ])
    const transactor2 = await Transactor(db)
    await transactor2.transact([['sky', 'color', 'blue']])
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('ensure schema is updated as facts are recorded', async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    try {
      await transactor.transact([['sky', 'color', 'blue']])
    } catch (e) {
      t.ok(e)
      t.equals(e.toString(), 'Error: Attribute not found: color')
    }
    await transactor.transact([
      ['01', '_db/attribute', 'color'],
      ['01', '_db/type', 'String']
    ])
    await transactor.transact([['sky', 'color', 'blue']])
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('ensure transact persists stuff to the db', async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    await transactor.transact([
      ['01', '_db/attribute', 'name'],
      ['01', '_db/type', 'String'],
      ['02', '_db/attribute', 'age'],
      ['02', '_db/type', 'Integer'],
      ['03', '_db/attribute', 'user_id'],
      ['03', '_db/type', 'Entity_ID']
    ])
    await transactor.transact(
      [
        ['0001', 'name', 'bob'],
        ['0001', 'age', 34],
        ['0002', 'name', 'jim'],
        ['0002', 'age', 23]
      ],
      {
        user_id: '0001'
      }
    )
    const allData = []
    db
      .readStream()
      .on('data', data => {
        allData.push(data)
      })
      .on('close', () => {
        t.equals(allData.length, 74)
        t.end()
      })
  } catch (e) {
    t.end(e)
  }
})

test('ensure transactor warms up with the latest transaction id', async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    await transactor.transact([
      ['01', '_db/attribute', 'is'],
      ['01', '_db/type', 'String']
    ])
    await transactor.transact([['bob', 'is', 'cool']])
    await transactor.transact([['bob', 'is', 'NOT cool']])
    await transactor.transact([['bob', 'is', 'cool']])
    const fb = transactor.connection.snap()
    const results1 = await q(fb, [['?_', '?_', '?_', '?txn']])
    const txns1 = _.unique(_.pluck(results1, '?txn')).sort()
    t.deepEqual(txns1, [1, 2, 3, 4])
    // warm up a new transactor to see where it picks up
    const transactor2 = await Transactor(db)
    const fb2 = await transactor2.transact([['bob', 'is', 'NOT cool']])
    const results2 = await q(fb2, [['?_', '?_', '?_', '?txn']])
    const txns2 = _.unique(_.pluck(results2, '?txn')).sort()
    t.deepEqual(txns2, [1, 2, 3, 4, 5])
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('transactions must be done serially, in the order they are recieved', async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)

    function transactAttr (attr) {
      return transactor
        .transact([['01', '_db/attribute', attr], ['01', '_db/type', 'String']])
        .then(fb => fb.txn)
        .catch(() => 'fail')
    }

    const results = await Promise.all([
      transactAttr('works'),
      transactAttr(111), //fails
      transactAttr('also works')
    ])
    t.deepEquals(results, [1, 'fail', 2])
    t.end()
  } catch (e) {
    t.end(e)
  }
})

async function setUpRetractTest (multiValued) {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    const fbs = [
      await transactor.transact([
        ['1', '_db/attribute', 'email'],
        ['1', '_db/type', 'String'],
        ['1', '_db/is-multi-valued', multiValued]
      ]),
      await transactor.transact([['bob', 'email', 'email@1']]),
      await transactor.transact([['bob', 'email', 'email@2']]),
      await transactor.transact([['bob', 'email', 'email@2', false]]),
      await transactor.transact([['bob', 'email', 'email@3']]),
      await transactor.transact([['bob', 'email', 'email@2']]),
      await transactor.transact([['bob', 'email', 'email@1', false]]),
      await transactor.transact([['bob', 'email', 'email@2', false]]),
      await transactor.transact([['bob', 'email', 'email@3', false]])
    ]
    const results = []
    for (const fb of fbs) {
      results.push(
        _.pluck(await q(fb, [['bob', 'email', '?email']]), '?email').sort()
      )
    }
    return results
  } catch (e) {
    throw e
  }
}

test('retracting facts', async t => {
  try {
    const emailsOverTime = await setUpRetractTest(false)
    t.deepEquals(emailsOverTime, [
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
  } catch (e) {
    t.end(e)
  }
})

test('retracting multi-valued facts', async t => {
  try {
    const emailsOverTime = await setUpRetractTest(true)
    t.deepEquals(emailsOverTime, [
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
  } catch (e) {
    t.end(e)
  }
})
