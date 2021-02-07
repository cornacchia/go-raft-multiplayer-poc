const sails = require('sails')

async function setCollection(req, res) {
  const idx = parseInt(req.body.idx)

  const result = await sails.hooks.logs.simulateLogsAction(idx)
  return res.send(result)
}

module.exports = setCollection