import q from './q'
import qTuple from './qTuple'
import getEntity from './getEntity'
import Transactor from './transactor'

export default function (db, options, onStartup) {
  if (arguments.length === 2) {
    onStartup = options
    options = {}
  }
  Transactor(db, options, (err, transactor) => {
    if (err) {
      return onStartup(err)
    }
    const { connection, connection: { asOf, snap }, transact } = transactor
    onStartup(null, { asOf, connection, getEntity, q, qTuple, snap, transact })
  })
}
