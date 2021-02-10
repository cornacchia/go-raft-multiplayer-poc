const sails = require('sails')

async function setCollection(req, res) {
  const collection = req.body.collection

  sails.hooks.analyze.setCurrentCollection(collection)
  const result = await sails.hooks.logs.setCurrentCollection(collection)
  return res.send(result)
}

module.exports = setCollection