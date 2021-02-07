const sails = require('sails')

function loadLogs (req, res) {
  const raftLogFolder = req.body.raftLogFolder
  const collectionName = req.body.collectionName
  sails.hooks.db.loadLogs(raftLogFolder, collectionName)
  return res.ok()
}

module.exports = loadLogs