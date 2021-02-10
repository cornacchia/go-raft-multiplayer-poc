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
  sails.log.debug(logHeader + 'Assign indexes to logs (this can take a while...)')
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
    .sort({t: 1})

  cursor.each((err, doc) => {
    if (err) throw err
    if (doc) jobQueue.push(doc)
  })
  
  jobQueue.drain(() => {
    loadingLogs = false
    sails.log.debug(logHeader + 'Done')
    error = ""
  })
}

function parseTimestamp(line, obj) {
  const timeMatch = sails.config.regex.time.exec(line)
  if (timeMatch) {
    const timeString = timeMatch[1]
    obj.t = new Date(Date.parse(timeString))
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
    obj.saer = { i : nodeId, f: firstIdx, l: lastIdx }
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
    obj.isr = { i : nodeId, li: lastIncludedIdx }
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
    obj.bc = true
  }
}

function parseBecomeLeader(line, obj) {
  const becomeLeaderMatch = sails.config.regex.becomeLeader.exec(line)
  if (becomeLeaderMatch) {
    const term = becomeLeaderMatch[1]
    const lastCommit = becomeLeaderMatch[2]
    obj.bl = {
      t: term,
      lc: lastCommit
    }
  }
}

function parseApplyLog(line, obj) {
  const applyLogMatch = sails.config.regex.applyLog.exec(line)
  if (applyLogMatch) {
    const idx = applyLogMatch[1]
    const log = applyLogMatch[2]
    obj.al = {
      i: idx,
      l: log
    }
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

  return result
}

async function loadLogFile(file, collection, nodeId) {
  sails.log.debug(logHeader + 'Loading log file: ', file)
  const newLogs = []
  const data = fs.readFileSync(file, 'UTF-8')
  const lines = data.split(/\r?\n/)

  lines.forEach(line => {
    var newLog = parseLogLine(line, nodeId)
    if (_.keys(newLog).length > 2) newLogs.push(newLog)
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

  assignIndexesToLogs(collection)
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

  loadLogsIntoCollection(dirPath, collection)
}

module.exports = function defineDbHook (sailsInstance) {
  if (!sails) sails = sailsInstance

  return {
    getState,
    loadLogs,
    getCollections
  }
}