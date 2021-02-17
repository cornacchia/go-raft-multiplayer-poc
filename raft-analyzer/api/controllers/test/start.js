const sails = require('sails')

function startTests (req, res) {
  res.ok()
  sails.hooks.test.startTests()
}

module.exports = startTests