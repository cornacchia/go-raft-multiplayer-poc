const sails = require('sails')

function stopTests (req, res) {
  res.ok()
  sails.hooks.test.stopTests()
}

module.exports = stopTests