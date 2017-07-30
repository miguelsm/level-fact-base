import _ from 'lodash'

const dbTypes = {
  Date: {
    validate: _.isDate,
    encode (d) {
      return d.toISOString()
    },
    decode (s) {
      return new Date(s)
    }
  },
  String: {
    validate: _.isString,
    encode: _.identity,
    decode: _.identity
  },
  Integer: {
    validate (n) {
      return _.isNumber(n) && n % 1 === 0
    },
    encode (n) {
      return n.toString()
    },
    decode (s) {
      return parseInt(s, 10) || 0
    }
  },
  Number: {
    validate: _.isNumber,
    encode (n) {
      return n.toString()
    },
    decode (s) {
      return parseFloat(s)
    }
  },
  Boolean: {
    validate (v) {
      return v === true || v === false
    },
    encode (v) {
      return v ? '1' : '0'
    },
    decode (s) {
      return s === '0' ? false : true
    }
  },
  Entity_ID: {
    validate: _.isString,
    encode: _.identity,
    decode: _.identity
  }
}

const dbSchema = [
  {
    '_db/attribute': '_db/attribute',
    '_db/type': 'String'
  },
  {
    '_db/attribute': '_db/type',
    '_db/type': 'String'
  },
  {
    '_db/attribute': '_db/is-multi-valued',
    '_db/type': 'Boolean'
  },
  {
    '_db/attribute': '_db/txn-time',
    '_db/type': 'Date'
  }
]

export default {
  dbSchema,
  dbTypes,
  indexNames: ['eavto', 'aveto', 'vaeto', 'teavo']
}
