import genInt from 'gent/generator/integer'
import genString from 'gent/generator/string'

const nextStr = genString(genInt(0, 1000))

export default function () {
  return nextStr.next().value
}
