const fs = require('fs')
const path = require('path')
const _ = require('lodash')
const async = require('async')
let sails

const logHeader = '[Hooks][Db] '

// DB State
let loadingLogs = false
let error = ""

function getState () {
  return {
    loadingLogs,
    error
  }
}

async function getCollections() {
  try {
    collections = await sails.getDatastore().manager.listCollections().toArray()
  } catch (err) {
    error = "Could not get list of database collections"
    sails.log.error(error, err)
    return err
  }

  return collections.map(col => col.name)
}

async function assignIndexesToLogs(collection) {
  sails.log.info(logHeader + 'Assign indexes to logs (this can take a while...)')
  let idx = 0
  const jobQueue = async.queue((doc, callback) => {
    // code for your update
    sails.getDatastore().manager.collection(collection).updateOne(
      { _id: doc._id },
      { $set: {i: idx++ } },
      callback)
  }, 1);

  var cursor = sails.getDatastore().manager.collection(collection)
    .find({})
    .sort({ts: 1})

  cursor.each((err, doc) => {
    if (err) throw err
    if (doc) jobQueue.push(doc)
  })

  await jobQueue.drain()
  loadingLogs = false
  sails.log.info(logHeader + 'Done')
  error = ""
}

function parseTimestamp(line, obj) {
  const timeMatch = sails.config.regex.time.exec(line)
  if (timeMatch) {
    const timeString = timeMatch[1]
    obj.t = new Date(Date.parse(timeString))
    const microseconds = parseInt(timeMatch[2])
    obj.ts = (obj.t.getTime() * 1000) + microseconds
  }
}

function parseListenerUp(line, obj) {
  const listenerMatch = sails.config.regex.listenerUp.exec(line)
  if (listenerMatch) {
    const nodeId = listenerMatch[1]
    obj.lu = { i : nodeId }
  }
}

function parseConnectedToNode(line, obj) {
  const connectedToNodeMatch = sails.config.regex.connectedToNode.exec(line)
  if (connectedToNodeMatch) {
    const nodeId = connectedToNodeMatch[1]
    obj.ctn = { i : nodeId }
  }
}

function parseSendAppendEntriesRPC(line, obj) {
  const sendAppendEntriesRPCMatch = sails.config.regex.sendAppendEntriesRPC.exec(line)
  if (sendAppendEntriesRPCMatch) {
    const nodeId = sendAppendEntriesRPCMatch[1]
    const firstIdx = sendAppendEntriesRPCMatch[2]
    const lastIdx = sendAppendEntriesRPCMatch[3]
    obj.saer = { i : nodeId, f: parseInt(firstIdx), l: parseInt(lastIdx) }
  }
}

function parseRespondAppendEntriesRPC(line, obj) {
  const respondAppendEntriesRPCMatch = sails.config.regex.respondAppendEntriesRPC.exec(line)
  if (respondAppendEntriesRPCMatch) {
    const nodeId = respondAppendEntriesRPCMatch[1]
    const success = respondAppendEntriesRPCMatch[2]
    obj.raer = { i : nodeId, s: success }
  }
}

function parseSendRequestVoteRPC(line, obj) {
  const sendRequestVoteRPCMatch = sails.config.regex.sendRequestVoteRPC.exec(line)
  if (sendRequestVoteRPCMatch) {
    const nodeId = sendRequestVoteRPCMatch[1]
    obj.srvr = { i : nodeId }
  }
}

function parseRespondRequestVoteRPC(line, obj) {
  const respondRequestVoteRPCMatch = sails.config.regex.respondRequestVoteRPC.exec(line)
  if (respondRequestVoteRPCMatch) {
    const nodeId = respondRequestVoteRPCMatch[1]
    const success = respondRequestVoteRPCMatch[2]
    obj.rrvr = { i : nodeId, s: success }
  }
}

function parseInstallSnapshotRPC(line, obj) {
  const installSnapshotRPCMatch = sails.config.regex.installSnapshotRPC.exec(line)
  if (installSnapshotRPCMatch) {
    const nodeId = installSnapshotRPCMatch[1]
    const lastIncludedIdx = installSnapshotRPCMatch[2]
    obj.isr = { i : nodeId, li: parseInt(lastIncludedIdx) }
  }
}

function parseRespondInstallSnapshotRPC(line, obj) {
  const respondInstallSnapshotRPCMatch = sails.config.regex.respondInstallSnapshotRPC.exec(line)
  if (respondInstallSnapshotRPCMatch) {
    const nodeId = respondInstallSnapshotRPCMatch[1]
    const success = respondInstallSnapshotRPCMatch[2]
    obj.risr = { i : nodeId, s: success }
  }
}

function parseBecomeFollower(line, obj) {
  const becomeFollowerMatch = sails.config.regex.becomeFollower.exec(line)
  if (becomeFollowerMatch) {
    obj.bf = true
  }
}

function parseBecomeCandidate(line, obj) {
  const becomeCandidateMatch = sails.config.regex.becomeCandidate.exec(line)
  if (becomeCandidateMatch) {
    const term = becomeCandidateMatch[1]
    obj.bc = { t: parseInt(term) }
  }
}

function parseBecomeLeader(line, obj) {
  const becomeLeaderMatch = sails.config.regex.becomeLeader.exec(line)
  if (becomeLeaderMatch) {
    const term = becomeLeaderMatch[1]
    obj.bl = { t: parseInt(term) }
  }
}

function parseApplyLog(line, obj) {
  const applyLogMatch = sails.config.regex.applyLog.exec(line)
  if (applyLogMatch) {
    const idx = applyLogMatch[1]
    const log = applyLogMatch[2]
    obj.al = { i: parseInt(idx), l: log }
  }
}

function parseShuttingDown(line, obj) {
  const shuttingDownMatch = sails.config.regex.shuttingDown.exec(line)
  if (shuttingDownMatch) {
    obj.sd = true
  }
}

function parseActionTimeout(line, obj) {
  const actionTimeoutMatch = sails.config.regex.actionTimeout.exec(line)
  if (actionTimeoutMatch) {
    const timeout = actionTimeoutMatch[1]
    obj.at = timeout
  }
}

function parseAddLog(line, obj) {
  const addLogMatch = sails.config.regex.addLog.exec(line)
  if (addLogMatch) {
    const logIdx = addLogMatch[1]
    const term = addLogMatch[2]
    const hash = addLogMatch[3]
    obj.adl = { i: parseInt(logIdx), t: parseInt(term), h: hash}
  }
}

function parseRemoveLogs(line, obj) {
  const removeLogMatch = sails.config.regex.removeLog.exec(line)
  if (removeLogMatch) {
    const startIdx = removeLogMatch[1]
    const endIdx = removeLogMatch[2]
    obj.rl = { si: startIdx, ei: endIdx }
  }
}

function parseLogLine(line, nodeId) {
  let result = { n: nodeId }
  parseTimestamp(line, result)
  parseListenerUp(line, result)
  parseConnectedToNode(line, result)
  parseSendAppendEntriesRPC(line, result)
  parseRespondAppendEntriesRPC(line, result)
  parseSendRequestVoteRPC(line, result)
  parseRespondRequestVoteRPC(line, result)
  parseInstallSnapshotRPC(line, result)
  parseRespondInstallSnapshotRPC(line, result)
  parseBecomeFollower(line, result)
  parseBecomeCandidate(line, result)
  parseBecomeLeader(line, result)
  parseApplyLog(line, result)
  parseShuttingDown(line, result)
  parseActionTimeout(line, result)
  parseAddLog(line, result)
  parseRemoveLogs(line, result)

  return result
}

async function loadLogFile(file, collection, nodeId) {
  sails.log.debug(logHeader + 'Loading log file: ', file)
  const newLogs = []
  const data = fs.readFileSync(file, 'UTF-8')
  const lines = data.split(/\r?\n/)

  lines.forEach(line => {
    var newLog = parseLogLine(line, nodeId)
    if (_.keys(newLog).length > 3) newLogs.push(newLog)
  })

  try {
    await sails.getDatastore().manager.collection(collection).insertMany(newLogs)
  } catch (err) {
    error = "Error inserting new logs into collection " + collection
    sails.log.error(error, err)
  }
}

// At this point we are sure that dirPath exists and collection does not exist in db
async function loadLogsIntoCollection(dirPath, collection) {
  await sails.getDatastore().manager.collection(collection).createIndex({ t: 1 })
  await sails.getDatastore().manager.collection(collection).createIndex({ ts: 1 })
  await sails.getDatastore().manager.collection(collection).createIndex({ i: 1 })
  await sails.getDatastore().manager.collection(collection).createIndex({ n: 1 })
  let files
  try {
    files = fs.readdirSync(dirPath)
  } catch (err) {
    error = "Could not read directory " + dirPath
    sails.log.error(error, err)
    return
  }

  for (let file of files) {
    const fileMatch = sails.config.regex.raftLogFile.exec(file)
    if (fileMatch) {
      await loadLogFile(path.join(dirPath, file), collection, fileMatch[1])
    }
  }

  await assignIndexesToLogs(collection)
}

async function loadLogs(dirPath, collection) {
  sails.log.info(logHeader + 'Loading raft logs from ' + dirPath + ' into collection ' + collection)
  loadingLogs = true

  if (!fs.existsSync(dirPath)) {
    error = "Path not found: " + dirPath
    return Error(error)
  }

  const collections = await getCollections()

  if (collections.includes(collection)) {
    sails.log.info(logHeader + 'Collection ' + collection + ' already exists in database, will drop it')
    try {
      await sails.getDatastore().manager.dropCollection(collection)
    } catch (err) {
      error = 'Could not drop collection ' + collection
      sails.log.error(error, err)
      return err
    }
  }

  await loadLogsIntoCollection(dirPath, collection)
}

module.exports = function defineDbHook (sailsInstance) {
  if (!sails) sails = sailsInstance

  return {
    getState,
    loadLogs,
    getCollections
  }
}