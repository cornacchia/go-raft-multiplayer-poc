const sails = require('sails')

async function getState (req, res) {
  const collections = await sails.hooks.db.getCollections()
  const dbState = sails.hooks.db.getState()

  return res.send({
    collections,
    dbState
  })
}

module.exports = getState