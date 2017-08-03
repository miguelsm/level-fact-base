import _ from 'lodash'
import level from 'levelup'
import memdown from 'memdown'
import test from 'tape'
import getEntity from '../src/get-entity'
import q from '../src/q'
import qTuple from '../src/q-tuple'
import Transactor from '../src/transactor'

async function setupMiddleDataset () {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    await transactor.transact([
      ['01', '_db/attribute', 'father'],
      ['01', '_db/type', 'String'],

      ['02', '_db/attribute', 'mother'],
      ['02', '_db/type', 'String']
    ])
    return await transactor.transact([
      ['axl', 'father', 'mike'],
      ['axl', 'mother', 'frankie'],
      ['sue', 'father', 'mike'],
      ['sue', 'mother', 'frankie'],
      ['brick', 'father', 'mike'],
      ['brick', 'mother', 'frankie'],
      ['mike', 'father', 'big mike'],
      ['rusty', 'father', 'big mike'],
      ['frankie', 'mother', 'pat'],
      ['frankie', 'father', 'tag'],
      ['janet', 'mother', 'pat'],
      ['janet', 'father', 'tag']
    ])
  } catch (e) {
    throw e
  }
}

const prophets = [
  'smith',
  'young',
  'taylor',
  'woodruff',
  'snow',
  'f. smith',
  'grant',
  'a. smith',
  'mckay',
  'fielding smith',
  'lee',
  'kimball',
  'benson',
  'hunter',
  'hinckley',
  'monson'
]

async function setupProphetDataset () {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    const fbs = [
      await transactor.transact([
        ['01', '_db/attribute', 'is'],
        ['01', '_db/type', 'String']
      ])
    ]
    for (const name of prophets) {
      fbs.push(await transactor.transact([['prophet', 'is', name]]))
    }
    return fbs
  } catch (e) {
    throw e
  }
}

test('basic qTuple stuff', async t => {
  try {
    const fb = await setupMiddleDataset()
    const [
      axl_mother,
      axl_relation_to_mike,
      mikes_children,
      axl_has_no_children
    ] = await Promise.all([
      qTuple(fb, ['axl', 'mother', '?mother']),
      qTuple(fb, ['axl', '?relation', 'mike']),
      qTuple(fb, ['?children', 'father', '?father'], {
        '?father': 'mike'
      }),
      qTuple(fb, ['?children', 'father', 'axl'])
    ])
    t.deepEqual(_.pluck(axl_mother, '?mother'), ['frankie'])
    t.deepEqual(_.pluck(axl_relation_to_mike, '?relation'), ['father'])
    t.deepEqual(_.pluck(mikes_children, '?children').sort(), [
      'axl',
      'brick',
      'sue'
    ])
    t.equal(axl_has_no_children.length, 0)
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('do some family tree questions', async t => {
  try {
    const fb = await setupMiddleDataset()
    const r = {
      husbands_and_wifes: await q(fb, [
        ['?child', 'mother', '?wife'],
        ['?child', 'father', '?husband']
      ]),
      sue_grandfathers: await q(
        fb,
        [
          ['sue', 'father', '?father'],
          ['sue', 'mother', '?mother'],
          ['?mother', 'father', '?grandpa1'],
          ['?father', 'father', '?grandpa2']
        ],
        [{}]
      ),
      sue_siblings: await q(
        fb,
        [['?sue', 'mother', '?mother'], ['?sibling', 'mother', '?mother']],
        [{ '?sue': 'sue' }]
      )
    }
    t.deepEqual(
      _.unique(
        _.map(
          r.husbands_and_wifes,
          result => result['?husband'] + ' & ' + result['?wife']
        ).sort()
      ),
      ['mike & frankie', 'tag & pat']
    )
    t.deepEqual(
      _.unique(
        _.pluck(r.sue_grandfathers, '?grandpa1').concat(
          _.pluck(r.sue_grandfathers, '?grandpa2')
        )
      ).sort(),
      ['big mike', 'tag']
    )
    t.deepEqual(_.unique(_.pluck(r.sue_siblings, '?sibling')).sort(), [
      'axl',
      'brick',
      'sue'
    ])
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('queries using txn', async t => {
  try {
    const fb_versions = await setupProphetDataset()
    const fb = _.last(fb_versions)
    const r = {
      first: await q(fb, [['prophet', 'is', '?name', 2]]),
      third: await q(fb, [['prophet', 'is', '?name', 4]]),
      when_was_young: await q(fb, [['prophet', 'is', 'young', '?txn']]),
      who_is_current: await q(fb, [['prophet', 'is', '?name']]),
      names_in_order: await q(fb, [['prophet', 'is', '?name', '?txn']])
    }
    t.deepEqual(_.pluck(r.first, '?name'), ['smith'])
    t.deepEqual(_.pluck(r.third, '?name'), ['taylor'])
    t.deepEqual(_.pluck(r.when_was_young, '?txn'), [3])
    t.deepEqual(_.pluck(r.who_is_current, '?name'), ['monson'])
    t.deepEqual(_.pluck(_.sortBy(r.names_in_order, '?txn'), '?name'), prophets)
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('queries using fb_versions', async t => {
  try {
    const fb_versions = await setupProphetDataset()
    const r = await Promise.all(
      fb_versions.map(fb => q(fb, [['prophet', 'is', '?name']]))
    )
    r.forEach((bindings, i) => {
      t.deepEqual(bindings, i === 0 ? [] : [{ '?name': prophets[i - 1] }])
    })
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('handle invalid fb', async t => {
  try {
    async function testFB (fb) {
      try {
        const r = {}
        try {
          r.q = await q(
            fb,
            [['?sue', 'mother', '?mother'], ['?sibling', 'mother', '?mother']],
            [{ '?sue': 'sue' }]
          )
        } catch (e) {
          r.q = e
        }
        try {
          r.qTuple = await qTuple(fb, ['axl', 'mother', '?mother'])
        } catch (e) {
          r.qTuple = e
        }
        try {
          r.getEntity = await getEntity(fb, 'axl')
        } catch (e) {
          r.getEntity = e
        }
        return r
      } catch (e) {
        throw e
      }
    }

    const fb = await setupMiddleDataset()
    const r = await Promise.all(
      [
        fb,
        null,
        undefined,
        10,
        true,
        fb.txn,
        {},
        { hindex: fb.hindex },
        ['one'],
        [[]]
      ].map(testFB)
    )
    // assert the valid fb works
    t.deepEqual(_.unique(_.pluck(r[0].q, '?sibling')).sort(), [
      'axl',
      'brick',
      'sue'
    ])
    t.deepEqual(r[0].qTuple, [{ '?mother': 'frankie' }])
    t.deepEqual(r[0].getEntity, { father: 'mike', mother: 'frankie' })

    // assert the rest all fail b/c fb is not valid
    _.each(_.rest(r), err => {
      t.deepEqual(err, {
        q: new Error('Must pass fb as the first argument'),
        qTuple: new Error('Must pass fb as the first argument'),
        getEntity: new Error('Must pass fb as the first argument')
      })
    })
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('the throw-away binding', async t => {
  try {
    const fb = await setupMiddleDataset()
    const [all_entities, all_fathers, sue_siblings] = await Promise.all([
      q(fb, [['?entity']]),
      q(fb, [['?_', 'father', '?father']]),
      q(
        fb,
        [['?sue', 'mother', '?_'], ['?sibling', 'mother', '?_']],
        [{ '?sue': 'sue' }]
      )
    ])
    t.deepEqual(_.pluck(all_entities, '?entity').sort(), [
      '01',
      '02',
      '_txid1',
      '_txid2',
      'axl',
      'brick',
      'frankie',
      'janet',
      'mike',
      'rusty',
      'sue'
    ])
    t.deepEqual(
      _.sortBy(all_fathers, '?father'),
      [{ '?father': 'big mike' }, { '?father': 'mike' }, { '?father': 'tag' }],
      'should not have ?_ bound to anything'
    )
    t.deepEqual(
      _.sortBy(sue_siblings, '?sibling'),
      [
        { '?sibling': 'axl', '?sue': 'sue' },
        { '?sibling': 'brick', '?sue': 'sue' },
        { '?sibling': 'frankie', '?sue': 'sue' },
        { '?sibling': 'janet', '?sue': 'sue' },
        { '?sibling': 'sue', '?sue': 'sue' }
      ],
      "should be everyone with a mother b/c ?_ shouldn't join"
    )
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test("escaping '?...' values", async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    await transactor.transact([
      ['0', '_db/attribute', 'name'],
      ['0', '_db/type', 'String']
    ])
    await transactor.transact([
      ['1', 'name', '?notavar'],
      ['2', 'name', 'notavar'],
      ['3', 'name', '\\?notavar'],
      ['4', 'name', '\\\\'],
      ['5', 'name', '?_']
    ])
    const fb = transactor.connection.snap()
    const [
      should_be_a_var,
      bind_it,
      escape_it,
      bind_it2,
      not_actually_escaped,
      double_slash,
      double_slash_bind,
      not_a_throw_away,
      not_a_throw_away2
    ] = await Promise.all([
      // should_be_a_var
      q(fb, [['?id', 'name', '?notavar']]),
      // bind_it
      q(fb, [['?id', 'name', '?name']], [{ '?name': '?notavar' }]),
      // escape_it
      q(fb, [['?id', 'name', '\\?notavar']]),
      // bind_it2
      q(fb, [['?id', 'name', '?name']], [{ '?name': '\\?notavar' }]),
      // not_actually_escaped
      q(fb, [['?id', 'name', '\\\\?notavar']]),
      // double_slash
      q(fb, [['?id', 'name', '\\\\\\']]),
      // double_slash_bind
      q(fb, [['?id', 'name', '?name']], [{ '?name': '\\\\' }]),
      // not_a_throw_away
      q(fb, [['?id', 'name', '\\?_']]),
      // not_a_throw_away2
      q(fb, [['?id', 'name', '?name']], [{ '?name': '?_' }])
    ])
    t.deepEqual(_.sortBy(should_be_a_var, '?id'), [
      { '?id': '1', '?notavar': '?notavar' },
      { '?id': '2', '?notavar': 'notavar' },
      { '?id': '3', '?notavar': '\\?notavar' },
      { '?id': '4', '?notavar': '\\\\' },
      { '?id': '5', '?notavar': '?_' }
    ])
    t.deepEqual(bind_it, [{ '?id': '1', '?name': '?notavar' }])
    t.deepEqual(escape_it, [{ '?id': '1' }])
    t.deepEqual(bind_it2, [{ '?id': '3', '?name': '\\?notavar' }])
    t.deepEqual(not_actually_escaped, [{ '?id': '3' }])
    t.deepEqual(double_slash, [{ '?id': '4' }])
    t.deepEqual(double_slash_bind, [{ '?id': '4', '?name': '\\\\' }])
    t.deepEqual(not_a_throw_away, [{ '?id': '5' }])
    t.deepEqual(not_a_throw_away2, [{ '?id': '5', '?name': '?_' }])
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('multi-valued attributes', async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    const fb_versions = [
      await transactor.transact([
        ['0', '_db/attribute', 'emails'],
        ['0', '_db/type', 'String'],
        ['0', '_db/is-multi-valued', true]
      ]),
      await transactor.transact([['me', 'emails', '1@email']]),
      await transactor.transact([
        ['me', 'emails', '2@email'],
        ['me', 'emails', '3@email']
      ])
    ]
    const fb = transactor.connection.snap()
    const [my_emails, the_first_me, the_last_me] = await Promise.all([
      q(fb, [['me', 'emails', '?emails']]),
      getEntity(fb_versions[1], 'me'),
      getEntity(fb, 'me')
    ])
    t.deepEqual(_.pluck(my_emails, '?emails'), [
      '1@email',
      '2@email',
      '3@email'
    ])
    t.deepEqual(the_first_me, { emails: ['1@email'] })
    t.deepEqual(the_last_me, {
      emails: ['1@email', '2@email', '3@email']
    })
    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('attribute type encoding/decoding', async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    await transactor.transact([
      ['s0', '_db/attribute', 'time'],
      ['s0', '_db/type', 'Date'],
      ['s0', '_db/is-multi-valued', true],
      ['s1', '_db/attribute', 'int'],
      ['s1', '_db/type', 'Integer'],
      ['s2', '_db/attribute', 'float'],
      ['s2', '_db/type', 'Number']
    ])
    await transactor.transact([['1', 'time', new Date(2010, 11, 25)]])
    await transactor.transact([['2', 'int', 123]])
    await transactor.transact([['3', 'float', 123.45]])
    const fb = transactor.connection.snap()
    t.ok(
      fb.schema.time['_db/is-multi-valued'] === true,
      'must also decode db default schema values'
    )

    const r = await Promise.all([
      q(fb, [['1', 'time', '?val']]), // time1
      q(fb, [['2', 'int', '?val']]), // integer1
      q(fb, [['3', 'float', '?val']]), // number1

      //query with variable attribute name
      q(fb, [['1', '?a', '?val']]), // time2
      q(fb, [['2', '?a', '?val']]), // integer2
      q(fb, [['3', '?a', '?val']]), // number2

      //query with unknown attribute name
      q(fb, [['1', '?_', '?val']]), // time3
      q(fb, [['2', '?_', '?val']]), // integer3
      q(fb, [['3', '?_', '?val']]), // number3

      //encode values at query with known attribute name
      q(fb, [['?e', 'time', new Date(2010, 11, 25)]]), // time4
      q(fb, [['?e', 'int', 123]]), // integer4
      q(fb, [['?e', 'float', 123.45]]), // number4

      //encode values at query with variable attribute name
      q(fb, [['?e', '?a', new Date(2010, 11, 25)]]), // time5
      q(fb, [['?e', '?a', 123]]), // integer5
      q(fb, [['?e', '?a', 123.45]]), // number5

      //encode values at query with unknown attribute name
      q(fb, [['?e', '?_', new Date(2010, 11, 25)]]), // time6
      q(fb, [['?e', '?_', 123]]), // integer6
      q(fb, [['?e', '?_', 123.45]]) // number6
    ])

    r.forEach(results => {
      t.equal(
        results.length,
        1,
        'all these type encode/decode queries should return 1 result'
      )
    })

    const [
      time1,
      integer1,
      number1,
      time2,
      integer2,
      number2,
      time3,
      integer3,
      number3,
      time4,
      integer4,
      number4,
      time5,
      integer5,
      number5,
      time6,
      integer6,
      number6
    ] = r

    t.ok(_.isDate(time1[0]['?val']))
    t.ok(_.isDate(time2[0]['?val']))
    t.ok(_.isDate(time3[0]['?val']))

    t.ok(_.isNumber(integer1[0]['?val']))
    t.ok(_.isNumber(integer2[0]['?val']))
    t.ok(_.isNumber(integer3[0]['?val']))
    t.equal(integer1[0]['?val'], 123)
    t.equal(integer2[0]['?val'], 123)
    t.equal(integer3[0]['?val'], 123)

    t.ok(_.isNumber(number1[0]['?val']))
    t.ok(_.isNumber(number2[0]['?val']))
    t.ok(_.isNumber(number3[0]['?val']))
    t.equal(number1[0]['?val'], 123.45)
    t.equal(number2[0]['?val'], 123.45)
    t.equal(number3[0]['?val'], 123.45)

    t.equal(time4[0]['?e'], '1')
    t.equal(time5[0]['?e'], '1')
    t.equal(time6[0]['?e'], '1')
    t.equal(integer4[0]['?e'], '2')
    t.equal(integer5[0]['?e'], '2')
    t.equal(integer6[0]['?e'], '2')
    t.equal(number4[0]['?e'], '3')
    t.equal(number5[0]['?e'], '3')
    t.equal(number6[0]['?e'], '3')

    t.end()
  } catch (e) {
    t.end(e)
  }
})

test('delayed join, and join order', async t => {
  try {
    const db = level(memdown)
    const transactor = await Transactor(db)
    await transactor.transact([
      ['0', '_db/attribute', '->'],
      ['0', '_db/type', 'Entity_ID'],
      ['0', '_db/is-multi-valued', true]
    ])
    const fb = await transactor.transact([
      ['a', '->', 'c'],
      ['a', '->', 'd'],
      ['a', '->', 'e'],

      ['b', '->', 'f'],
      ['b', '->', 'g'],
      ['b', '->', 'h'],

      ['d', '->', 'g']
    ])
    const [
      one_row,
      no_join,
      da_join,
      da_join_reverse,
      da_join_mix
    ] = await Promise.all([
      // one_row
      q(fb, [['a', '->', '?va']]),
      // no_join
      q(fb, [['a', '->', '?va'], ['b', '->', '?vb']]),
      // da_join
      q(fb, [['a', '->', '?va'], ['b', '->', '?vb'], ['?va', '->', '?vb']]),
      // da_join_reverse
      q(fb, [['?va', '->', '?vb'], ['a', '->', '?va'], ['b', '->', '?vb']]),
      // da_join_mix
      q(fb, [['a', '->', '?va'], ['?va', '->', '?vb'], ['b', '->', '?vb']])
    ])
    t.equal(one_row.length, 3, 'should return everthing a points to')
    t.equal(
      no_join.length,
      9,
      'should return every combination of a and b pointers'
    )

    t.deepEqual(da_join, [{ '?va': 'd', '?vb': 'g' }])
    t.deepEqual(da_join_reverse, da_join, "q tuple order shouldn't matter")
    t.deepEqual(da_join_mix, da_join, "q tuple order shouldn't matter")

    t.end()
  } catch (e) {
    t.end(e)
  }
})
