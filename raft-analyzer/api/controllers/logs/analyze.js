const sails = require('sails')

function loadLogs (req, res) {
  sails.hooks.analyze.runAnalysis()
  return res.ok()
}

module.exports = loadLogs