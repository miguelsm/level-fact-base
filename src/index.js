import q from './q'
import qTuple from './q-tuple'
import getEntity from './get-entity'
import Transactor from './transactor'

export default async function (db, options = {}) {
  try {
    const transactor = await Transactor(db, options)
    const { connection, connection: { asOf, snap }, transact } = transactor
    return { asOf, connection, getEntity, q, qTuple, snap, transact }
  } catch (e) {
    throw e
  }
}
