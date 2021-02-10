const sails = require('sails')

async function getState (req, res) {
  const collections = await sails.hooks.db.getCollections()
  const dbState = sails.hooks.db.getState()
  const analyze = sails.hooks.analyze.getState()

  return res.send({
    collections,
    dbState,
    analyze
  })
}

module.exports = getState