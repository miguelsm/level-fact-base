import _ from 'lodash'
import contra from 'contra'

export default function (worker) {
  const qData = {}

  const q = contra.queue((q_id, callback) => {
    worker(qData[q_id], callback)
    delete qData[q_id]
  })

  return {
    push (data, callback) {
      const q_id = _.uniqueId()
      qData[q_id] = data
      q.unshift(q_id, callback)
    }
  }
}
