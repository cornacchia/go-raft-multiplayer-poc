/**
 * > Election Safety: at most one leader can be elected in a given term.
 * > Leader Append-Only: a leader never overwrites or deletes entries in its log;
 * it only appends new entries.
 * > Log Matching: if two logs contain an entry with the same index and term,
 * then the logs are identical in all entries up through the given index.
 * > Leader Completeness: if a log entry is committed in a given term, then that entry
 * will be present in the logs of the leaders for all higher-numbered terms.
 * > State Machine Safety: if a server has applied a log entry at a given index
 * to its state machine, no other server will ever apply a different log entry for the same index.
 */
const _ = require('lodash')
const logHeader = '[Hooks][Analysis] '

let sails
let runningAnalysis = false
let reset = false
let currentCollection = ""

let currentAnalysis = {
  lastLeaderTerm: -1,
  lastCommittedLog: -1,
  violations: {
    electionSafety: [],
    stateMachineSafety: [],
    leaderCompleteness: []
  },
  appliedLogs: {}
}
const fieldsToSendToClient = ['lastLeaderTerm', 'lastCommittedLog', 'violations']

function resetAnalysis() {
  currentAnalysis = {
    lastLeaderTerm: -1,
    lastCommittedLog: -1,
    violations: {
      electionSafety: [],
      stateMachineSafety: [],
      leaderCompleteness: []
    },
    appliedLogs: {}
  }
}

function setCurrentCollection(newCollection) {
  if (newCollection !== currentCollection) {
    currentCollection = newCollection
    reset = true
  }
}

function getState() {
  return {
    runningAnalysis,
    currentAnalysis: _.pick(currentAnalysis, fieldsToSendToClient)
  }
}

async function runAnalysis() {
  if (currentCollection === "") return
  sails.log.info(logHeader + 'Run analysis for collection ' + currentCollection)
  runningAnalysis = true
  reset = false
  let finish = false
  let idx = 0
  while (!finish && !reset) {
    const log = await sails.getDatastore().manager.collection(currentCollection).findOne({ i: idx })
    if (!log) finish = true
    else {
      if (log.bl) {
        // Election Safety check
        // The logs are accessed in order, we expect the new leader terms to be ordered as well
        if (log.bl.t === currentAnalysis.lastLeaderTerm) {
          sails.log.warn(logHeader + 'Election safety violation')
          currentAnalysis.violations.electionSafety.push({
            lastLeaderTerm: currentAnalysis.lastLeaderTerm,
            log: log
          })
        }

        // Leader Completeness check
        if (log.bl.lc < currentAnalysis.lastCommittedLog) {
          sails.log.warn(logHeader + 'Leader completeness violation')
          currentAnalysis.violations.leaderCompleteness.push({
            lastCommittedLog: currentAnalysis.lastCommittedLog,
            log: log
          })
        }

        currentAnalysis.lastLeaderTerm = log.bl.t
      }

      // State Machine Safety check
      if (log.al) {
        if (currentAnalysis.appliedLogs[log.al.i]) {
         if (currentAnalysis.appliedLogs[log.al.i] !== log.al.l) {
          sails.log.warn(logHeader + 'State machine safety violation')
          console.log(currentAnalysis.appliedLogs[log.al.i])
          console.log(log.al.l)
          currentAnalysis.violations.stateMachineSafety.push({
            oldLog: currentAnalysis.appliedLogs[log.al.i],
            log: log.al.l
          })
          }
        } else currentAnalysis.appliedLogs[log.al.i] = log.al.l

        if (log.al.i > currentAnalysis.lastCommittedLog) {
          currentAnalysis.lastCommittedLog = log.al.i
        }
      }

      idx += 1
    }
  }

  if (reset) resetAnalysis()
  runningAnalysis = false
  sails.log.info(logHeader + 'Analysis done for collection ' + currentCollection)
}

module.exports = function defineAnalyzeHook (sailsInstance) {
  if (!sails) sails = sailsInstance

  return {
    setCurrentCollection,
    runAnalysis,
    getState
  }
}