import _ from 'lodash'
import contra from 'contra'
import level from 'levelup'
import memdown from 'memdown'
import test from 'tape'
import getEntity from '../src/getEntity'
import q from '../src/q'
import qTuple from '../src/qTuple'
import Transactor from '../src/transactor'
import genRandomString from '../src/utils/genRandomString'

function setupMiddleDataset (callback) {
  const db = level(memdown)
  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return callback(err)
    }

    transactor.transact(
      [
        ['01', '_db/attribute', 'father'],
        ['01', '_db/type', 'String'],

        ['02', '_db/attribute', 'mother'],
        ['02', '_db/type', 'String']
      ],
      {},
      err => {
        if (err) {
          return callback(err)
        }
        transactor.transact(
          [
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
          ],
          {},
          callback
        )
      }
    )
  })
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

function setupProphetDataset (callback) {
  const db = level(memdown)
  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return callback(err)
    }
    contra.series(
      [
        contra.curry(
          transactor.transact,
          [['01', '_db/attribute', 'is'], ['01', '_db/type', 'String']],
          {}
        )
      ].concat(
        prophets.map(name => {
          return contra.curry(
            transactor.transact,
            [['prophet', 'is', name]],
            {}
          )
        })
      ),
      callback
    )
  })
}

test('basic qTuple stuff', t => {
  setupMiddleDataset((err, fb) => {
    if (err) {
      return t.end(err)
    }
    contra.concurrent(
      {
        axl_mother: contra.curry(qTuple, fb, ['axl', 'mother', '?mother']),
        axl_relation_to_mike: contra.curry(
          qTuple,
          fb,
          ['axl', '?relation', 'mike'],
          {}
        ),
        mikes_children: contra.curry(
          qTuple,
          fb,
          ['?children', 'father', '?father'],
          { '?father': 'mike' }
        ),
        axl_has_no_children: contra.curry(qTuple, fb, [
          '?children',
          'father',
          'axl'
        ])
      },
      (err, r) => {
        t.deepEqual(_.pluck(r.axl_mother, '?mother'), ['frankie'])
        t.deepEqual(_.pluck(r.axl_relation_to_mike, '?relation'), ['father'])
        t.deepEqual(_.pluck(r.mikes_children, '?children').sort(), [
          'axl',
          'brick',
          'sue'
        ])
        t.equal(r.axl_has_no_children.length, 0)
        t.end(err)
      }
    )
  })
})

test('do some family tree questions', t => {
  setupMiddleDataset((err, fb) => {
    if (err) {
      return t.end(err)
    }
    contra.concurrent(
      {
        husbands_and_wifes: contra.curry(q, fb, [
          ['?child', 'mother', '?wife'],
          ['?child', 'father', '?husband']
        ]),

        sue_grandfathers: contra.curry(
          q,
          fb,
          [
            ['sue', 'father', '?father'],
            ['sue', 'mother', '?mother'],
            ['?mother', 'father', '?grandpa1'],
            ['?father', 'father', '?grandpa2']
          ],
          [{}]
        ),

        sue_siblings: contra.curry(
          q,
          fb,
          [['?sue', 'mother', '?mother'], ['?sibling', 'mother', '?mother']],
          [{ '?sue': 'sue' }]
        )
      },
      (err, r) => {
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
        t.end(err)
      }
    )
  })
})

test('queries using txn', t => {
  setupProphetDataset((err, fb_versions) => {
    if (err) {
      return t.end(err)
    }
    const fb = _.last(fb_versions)
    contra.concurrent(
      {
        first: contra.curry(q, fb, [['prophet', 'is', '?name', 2]]),
        third: contra.curry(q, fb, [['prophet', 'is', '?name', 4]]),
        when_was_young: contra.curry(q, fb, [
          ['prophet', 'is', 'young', '?txn']
        ]),
        who_is_current: contra.curry(q, fb, [['prophet', 'is', '?name']]),
        names_in_order: contra.curry(q, fb, [
          ['prophet', 'is', '?name', '?txn']
        ])
      },
      (err, r) => {
        t.deepEqual(_.pluck(r.first, '?name'), ['smith'])
        t.deepEqual(_.pluck(r.third, '?name'), ['taylor'])
        t.deepEqual(_.pluck(r.when_was_young, '?txn'), [3])
        t.deepEqual(_.pluck(r.who_is_current, '?name'), ['monson'])
        t.deepEqual(
          _.pluck(_.sortBy(r.names_in_order, '?txn'), '?name'),
          prophets
        )
        t.end(err)
      }
    )
  })
})

test('queries using fb_versions', t => {
  setupProphetDataset((err, fb_versions) => {
    if (err) {
      return t.end(err)
    }
    contra.map(
      fb_versions,
      (fb, callback) => {
        //run the same query on each version of the db
        q(fb, [['prophet', 'is', '?name']], callback)
      },
      (err, r) => {
        r.map((bindings, i) => {
          t.deepEqual(bindings, i === 0 ? [] : [{ '?name': prophets[i - 1] }])
        })
        t.end(err)
      }
    )
  })
})

test('handle invalid fb', t => {
  function errPassingCurry () {
    const args = _.toArray(arguments)
    const fn = _.first(args)
    const fn_args = _.rest(args)
    return callback => {
      fn_args.push((err, o) => {
        callback(null, err ? err : o)
      })
      fn.apply(null, fn_args)
    }
  }

  function testFB (fb, callback) {
    contra.concurrent(
      {
        q: errPassingCurry(
          q,
          fb,
          [['?sue', 'mother', '?mother'], ['?sibling', 'mother', '?mother']],
          [{ '?sue': 'sue' }]
        ),
        qTuple: errPassingCurry(qTuple, fb, ['axl', 'mother', '?mother']),
        getEntity: errPassingCurry(getEntity, fb, 'axl')
      },
      callback
    )
  }

  setupMiddleDataset((err, fb) => {
    if (err) {
      return t.end(err)
    }
    contra.map(
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
      ],
      testFB,
      (err, r) => {
        //assert the valid fb works
        t.deepEqual(_.unique(_.pluck(r[0].q, '?sibling')).sort(), [
          'axl',
          'brick',
          'sue'
        ])
        t.deepEqual(r[0].qTuple, [{ '?mother': 'frankie' }])
        t.deepEqual(r[0].getEntity, { father: 'mike', mother: 'frankie' })

        //assert the rest all fail b/c fb is not valid
        _.each(_.rest(r), err => {
          t.deepEqual(err, {
            q: new Error('Must pass fb as the first argument'),
            qTuple: new Error('Must pass fb as the first argument'),
            getEntity: new Error('Must pass fb as the first argument')
          })
        })
        t.end(err)
      }
    )
  })
})

test('the throw-away binding', t => {
  setupMiddleDataset((err, fb) => {
    if (err) {
      return t.end(err)
    }
    contra.concurrent(
      {
        all_entities: contra.curry(q, fb, [['?entity']]),
        all_fathers: contra.curry(q, fb, [['?_', 'father', '?father']]),
        sue_siblings: contra.curry(
          q,
          fb,
          [['?sue', 'mother', '?_'], ['?sibling', 'mother', '?_']],
          [{ '?sue': 'sue' }]
        )
      },
      (err, r) => {
        t.deepEqual(_.pluck(r.all_entities, '?entity').sort(), [
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
          _.sortBy(r.all_fathers, '?father'),
          [
            { '?father': 'big mike' },
            { '?father': 'mike' },
            { '?father': 'tag' }
          ],
          'should not have ?_ bound to anything'
        )
        t.deepEqual(
          _.sortBy(r.sue_siblings, '?sibling'),
          [
            { '?sibling': 'axl', '?sue': 'sue' },
            { '?sibling': 'brick', '?sue': 'sue' },
            { '?sibling': 'frankie', '?sue': 'sue' },
            { '?sibling': 'janet', '?sue': 'sue' },
            { '?sibling': 'sue', '?sue': 'sue' }
          ],
          "should be everyone with a mother b/c ?_ shouldn't join"
        )
        t.end(err)
      }
    )
  })
})

test("escaping '?...' values", t => {
  const db = level(memdown)
  Transactor(db, {}, (err, transactor) => {
    if (err) {
      return t.end(err)
    }
    contra.series(
      [
        contra.curry(transactor.transact, [
          ['0', '_db/attribute', 'name'],
          ['0', '_db/type', 'String']
        ]),

        contra.curry(transactor.transact, [
          ['1', 'name', '?notavar'],
          ['2', 'name', 'notavar'],
          ['3', 'name', '\\?notavar'],
          ['4', 'name', '\\\\'],
          ['5', 'name', '?_']
        ])
      ],
      err => {
        if (err) {
          return t.end(err)
        }
        const fb = transactor.connection.snap()
        contra.concurrent(
          {
            should_be_a_var: contra.curry(q, fb, [['?id', 'name', '?notavar']]),
            bind_it: contra.curry(
              q,
              fb,
              [['?id', 'name', '?name']],
              [{ '?name': '?notavar' }]
            ),
            escape_it: contra.curry(q, fb, [['?id', 'name', '\\?notavar']]),
            bind_it2: contra.curry(
              q,
              fb,
              [['?id', 'name', '?name']],
              [{ '?name': '\\?notavar' }]
            ),
            not_actually_escaped: contra.curry(q, fb, [
              ['?id', 'name', '\\\\?notavar']
            ]),
            double_slash: contra.curry(q, fb, [['?id', 'name', '\\\\\\']]),
            double_slash_bind: contra.curry(
              q,
              fb,
              [['?id', 'name', '?name']],
              [{ '?name': '\\\\' }]
            ),
            not_a_throw_away: contra.curry(q, fb, [['?id', 'name', '\\?_']]),
            not_a_throw_away2: contra.curry(
              q,
              fb,
              [['?id', 'name', '?name']],
              [{ '?name': '?_' }]
            )
          },
          (err, r) => {
            t.deepEqual(_.sortBy(r.should_be_a_var, '?id'), [
              { '?id': '1', '?notavar': '?notavar' },
              { '?id': '2', '?notavar': 'notavar' },
              { '?id': '3', '?notavar': '\\?notavar' },
              { '?id': '4', '?notavar': '\\\\' },
              { '?id': '5', '?notavar': '?_' }
            ])
            t.deepEqual(r.bind_it, [{ '?id': '1', '?name': '?notavar' }])
            t.deepEqual(r.escape_it, [{ '?id': '1' }])
            t.deepEqual(r.bind_it2, [{ '?id': '3', '?name': '\\?notavar' }])
            t.deepEqual(r.not_actually_escaped, [{ '?id': '3' }])
            t.deepEqual(r.double_slash, [{ '?id': '4' }])
            t.deepEqual(r.double_slash_bind, [{ '?id': '4', '?name': '\\\\' }])
            t.deepEqual(r.not_a_throw_away, [{ '?id': '5' }])
            t.deepEqual(r.not_a_throw_away2, [{ '?id': '5', '?name': '?_' }])
            t.end(err)
          }
        )
      }
    )
  })
})

test('multi-valued attributes', t => {
  const db = level(memdown)
  Transactor(db, (err, transactor) => {
    if (err) {
      return t.end(err)
    }

    contra.series(
      [
        contra.curry(transactor.transact, [
          ['0', '_db/attribute', 'emails'],
          ['0', '_db/type', 'String'],
          ['0', '_db/is-multi-valued', true]
        ]),

        contra.curry(transactor.transact, [['me', 'emails', '1@email']]),
        contra.curry(transactor.transact, [
          ['me', 'emails', '2@email'],
          ['me', 'emails', '3@email']
        ])
      ],
      (err, fb_versions) => {
        if (err) {
          return t.end(err)
        }
        const fb = transactor.connection.snap()

        contra.concurrent(
          {
            my_emails: contra.curry(q, fb, [['me', 'emails', '?emails']]),
            the_first_me: contra.curry(getEntity, fb_versions[1], 'me'),
            the_last_me: contra.curry(getEntity, fb, 'me')
          },
          (err, r) => {
            if (err) {
              return t.end(err)
            }

            t.deepEqual(_.pluck(r.my_emails, '?emails'), [
              '1@email',
              '2@email',
              '3@email'
            ])
            t.deepEqual(r.the_first_me, { emails: ['1@email'] })
            t.deepEqual(r.the_last_me, {
              emails: ['1@email', '2@email', '3@email']
            })

            t.end()
          }
        )
      }
    )
  })
})

test('attribute type encoding/decoding', t => {
  const db = level(memdown)
  Transactor(db, (err, transactor) => {
    if (err) {
      return t.end(err)
    }

    contra.series(
      [
        contra.curry(transactor.transact, [
          ['s0', '_db/attribute', 'time'],
          ['s0', '_db/type', 'Date'],
          ['s0', '_db/is-multi-valued', true],

          ['s1', '_db/attribute', 'int'],
          ['s1', '_db/type', 'Integer'],

          ['s2', '_db/attribute', 'float'],
          ['s2', '_db/type', 'Number']
        ]),

        contra.curry(transactor.transact, [
          ['1', 'time', new Date(2010, 11, 25)]
        ]),
        contra.curry(transactor.transact, [['2', 'int', 123]]),
        contra.curry(transactor.transact, [['3', 'float', 123.45]])
      ],
      (err, fb_versions) => {
        if (err) {
          return t.end(err)
        }
        const fb = transactor.connection.snap()

        t.ok(
          fb.schema.time['_db/is-multi-valued'] === true,
          'must also decode db default schema values'
        )

        contra.concurrent(
          {
            time1: contra.curry(q, fb, [['1', 'time', '?val']]),
            integer1: contra.curry(q, fb, [['2', 'int', '?val']]),
            number1: contra.curry(q, fb, [['3', 'float', '?val']]),

            //query with variable attribute name
            time2: contra.curry(q, fb, [['1', '?a', '?val']]),
            integer2: contra.curry(q, fb, [['2', '?a', '?val']]),
            number2: contra.curry(q, fb, [['3', '?a', '?val']]),

            //query with unknown attribute name
            time3: contra.curry(q, fb, [['1', '?_', '?val']]),
            integer3: contra.curry(q, fb, [['2', '?_', '?val']]),
            number3: contra.curry(q, fb, [['3', '?_', '?val']]),

            //encode values at query with known attribute name
            time4: contra.curry(q, fb, [
              ['?e', 'time', new Date(2010, 11, 25)]
            ]),
            integer4: contra.curry(q, fb, [['?e', 'int', 123]]),
            number4: contra.curry(q, fb, [['?e', 'float', 123.45]]),

            //encode values at query with variable attribute name
            time5: contra.curry(q, fb, [['?e', '?a', new Date(2010, 11, 25)]]),
            integer5: contra.curry(q, fb, [['?e', '?a', 123]]),
            number5: contra.curry(q, fb, [['?e', '?a', 123.45]]),

            //encode values at query with unknown attribute name
            time6: contra.curry(q, fb, [['?e', '?_', new Date(2010, 11, 25)]]),
            integer6: contra.curry(q, fb, [['?e', '?_', 123]]),
            number6: contra.curry(q, fb, [['?e', '?_', 123.45]])
          },
          (err, r) => {
            if (err) {
              return t.end(err)
            }

            _.each(r, (results, key) => {
              t.equal(
                results.length,
                1,
                'all these type encode/decode queries should return 1 result'
              )
            })

            t.ok(_.isDate(r.time1[0]['?val']))
            t.ok(_.isDate(r.time2[0]['?val']))
            t.ok(_.isDate(r.time3[0]['?val']))

            t.ok(_.isNumber(r.integer1[0]['?val']))
            t.ok(_.isNumber(r.integer2[0]['?val']))
            t.ok(_.isNumber(r.integer3[0]['?val']))
            t.equal(r.integer1[0]['?val'], 123)
            t.equal(r.integer2[0]['?val'], 123)
            t.equal(r.integer3[0]['?val'], 123)

            t.ok(_.isNumber(r.number1[0]['?val']))
            t.ok(_.isNumber(r.number2[0]['?val']))
            t.ok(_.isNumber(r.number3[0]['?val']))
            t.equal(r.number1[0]['?val'], 123.45)
            t.equal(r.number2[0]['?val'], 123.45)
            t.equal(r.number3[0]['?val'], 123.45)

            t.equal(r.time4[0]['?e'], '1')
            t.equal(r.time5[0]['?e'], '1')
            t.equal(r.time6[0]['?e'], '1')
            t.equal(r.integer4[0]['?e'], '2')
            t.equal(r.integer5[0]['?e'], '2')
            t.equal(r.integer6[0]['?e'], '2')
            t.equal(r.number4[0]['?e'], '3')
            t.equal(r.number5[0]['?e'], '3')
            t.equal(r.number6[0]['?e'], '3')

            t.end()
          }
        )
      }
    )
  })
})

test('delayed join, and join order', t => {
  const db = level(memdown)
  Transactor(db, (err, transactor) => {
    if (err) {
      return t.end(err)
    }

    transactor.transact(
      [
        ['0', '_db/attribute', '->'],
        ['0', '_db/type', 'Entity_ID'],
        ['0', '_db/is-multi-valued', true]
      ],
      err => {
        if (err) {
          return t.end(err)
        }
        transactor.transact(
          [
            ['a', '->', 'c'],
            ['a', '->', 'd'],
            ['a', '->', 'e'],

            ['b', '->', 'f'],
            ['b', '->', 'g'],
            ['b', '->', 'h'],

            ['d', '->', 'g']
          ],
          (err, fb) => {
            if (err) {
              return t.end(err)
            }
            contra.concurrent(
              {
                one_row: contra.curry(q, fb, [['a', '->', '?va']]),

                no_join: contra.curry(q, fb, [
                  ['a', '->', '?va'],
                  ['b', '->', '?vb']
                ]),

                da_join: contra.curry(q, fb, [
                  ['a', '->', '?va'],
                  ['b', '->', '?vb'],
                  ['?va', '->', '?vb']
                ]),

                da_join_reverse: contra.curry(q, fb, [
                  ['?va', '->', '?vb'],
                  ['a', '->', '?va'],
                  ['b', '->', '?vb']
                ]),

                da_join_mix: contra.curry(q, fb, [
                  ['a', '->', '?va'],
                  ['?va', '->', '?vb'],
                  ['b', '->', '?vb']
                ])
              },
              (err, r) => {
                if (err) {
                  return t.end(err)
                }
                t.equal(
                  r.one_row.length,
                  3,
                  'should return everthing a points to'
                )
                t.equal(
                  r.no_join.length,
                  9,
                  'should return every combination of a and b pointers'
                )

                t.deepEqual(r.da_join, [{ '?va': 'd', '?vb': 'g' }])
                t.deepEqual(
                  r.da_join_reverse,
                  r.da_join,
                  "q tuple order shouldn't matter"
                )
                t.deepEqual(
                  r.da_join_mix,
                  r.da_join,
                  "q tuple order shouldn't matter"
                )

                t.end()
              }
            )
          }
        )
      }
    )
  })
})
