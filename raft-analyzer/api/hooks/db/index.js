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
  })
}

function parseLogLine(line) {
  let result = {}
  // Parse timestamp
  const timeMatch = sails.config.regex.time.exec(line)
  if (timeMatch) {
    const timeString = timeMatch[1]
    result.t = new Date(Date.parse(timeString))
  }
  return result
}

async function loadLogFile(file, collection) {
  sails.log.debug(logHeader + 'Loading log file: ', file)
  const newLogs = []
  const data = fs.readFileSync(file, 'UTF-8')
  const lines = data.split(/\r?\n/)

  lines.forEach(line => {
    var newLog = parseLogLine(line)
    if (!_.isEmpty(newLog)) newLogs.push(newLog)
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
  await sails.getDatastore().manager.collection(collection).createIndex({ t: 1, i: 1 })
  let files
  try {
    files = fs.readdirSync(dirPath)
  } catch (err) {
    error = "Could not read directory " + dirPath
    sails.log.error(error, err)
    return
  }

  for (let file of files) {
    if (sails.config.regex.raftLogFile.test(file)) await loadLogFile(path.join(dirPath, file), collection)
  }

  assignIndexesToLogs(collection)
}

function findCollection(collections, collection) {
  let found = false
  for (const coll of collections) {
    if (coll.name === collection) {
      found = true
      break
    }
  }
  return found
}

async function loadLogs(dirPath, collection) {
  sails.log.info(logHeader + 'Loading raft logs from ' + dirPath + ' into collection ' + collection)
  loadingLogs = true

  if (!fs.existsSync(dirPath)) {
    error = "Path not found: " + dirPath
    return Error(error)
  }

  let collections

  try {
    collections = await sails.getDatastore().manager.listCollections().toArray()
  } catch (err) {
    error = "Could not get list of database collections"
    sails.log.error(error, err)
    return err
  }

  if (findCollection(collections, collection)) {
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
    loadLogs
  }
}