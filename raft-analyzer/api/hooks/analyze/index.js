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
    leaderCompleteness: [],
    logMatching: []
  },
  appliedLogs: {},
  mainEvents: [],
  nodeStatus: {},
  logHash: {}
}
const fieldsToSendToClient = ['lastLeaderTerm', 'lastCommittedLog', 'violations', 'mainEvents']

function resetAnalysis() {
  currentAnalysis = {
    lastLeaderTerm: -1,
    lastCommittedLog: -1,
    violations: {
      electionSafety: [],
      stateMachineSafety: [],
      leaderCompleteness: [],
      leaderAppendOnly: [],
      logMatching: []
    },
    addedLogs: {},
    appliedLogs: {},
    mainEvents: [],
    nodeStatus: {},
    logHash: {}
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
  resetAnalysis()
  sails.log.info(logHeader + 'Run analysis for collection ' + currentCollection)
  runningAnalysis = true
  reset = false
  let finish = false
  let idx = 0
  while (!finish && !reset) {
    const log = await sails.getDatastore().manager.collection(currentCollection).findOne({ i: idx })
    if (!log) finish = true
    else {
      if (log.lu) {
        currentAnalysis.logHash[log.n] = {}
        currentAnalysis.nodeStatus[log.n] = { status: 'Follower', idx: log.i }
        currentAnalysis.mainEvents.push(log)
      }

      if (log.bf) currentAnalysis.nodeStatus[log.n] = { status: 'Follower', idx: log.i }

      if (log.bc) {
        currentAnalysis.nodeStatus[log.n] = { status: 'Candidate', idx: log.i }
        currentAnalysis.mainEvents.push(log)
      }

      if (log.sd) currentAnalysis.mainEvents.push(log)

      if (log.bl) {
        currentAnalysis.nodeStatus[log.n] = { status: 'Leader', idx: log.i }
        currentAnalysis.mainEvents.push(log)
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
        if (!currentAnalysis.addedLogs[log.n] && currentAnalysis.lastCommittedLog >= 0) {
          sails.log.warn(logHeader + 'Leader completeness anomaly')
          const msg = log.n + ' became leader with no logs'
          sails.log.debug(logHeader + msg)
        }
        if (currentAnalysis.addedLogs[log.n] && currentAnalysis.addedLogs[log.n].i < currentAnalysis.lastCommittedLog) {
          sails.log.warn(logHeader + 'Leader completeness violation')
          sails.log.debug(logHeader + 'Last leader log (' + log.n + '): ' + currentAnalysis.addedLogs[log.n].i)
          sails.log.debug(logHeader + 'Last committed log: ' + currentAnalysis.lastCommittedLog)
          currentAnalysis.violations.leaderCompleteness.push({
            lastCommittedLog: currentAnalysis.lastCommittedLog,
            lastLeaderLog: currentAnalysis.addedLogs[log.n].i
          })
        }

        currentAnalysis.lastLeaderTerm = log.bl.t
      }

      // State Machine Safety check
      if (log.al) {
        if (currentAnalysis.appliedLogs[log.al.i]) {
         if (currentAnalysis.appliedLogs[log.al.i] !== log.al.l) {
          sails.log.warn(logHeader + 'State machine safety violation')
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

      if (log.adl) {
        currentAnalysis.addedLogs[log.n] = log.adl
        if (!currentAnalysis.logHash[log.n]) currentAnalysis.logHash[log.n] = {}
        if (!currentAnalysis.logHash[log.n][log.adl.t]) currentAnalysis.logHash[log.n][log.adl.t] = {}
        if (!currentAnalysis.logHash[log.n][log.adl.t][log.adl.i]) currentAnalysis.logHash[log.n][log.adl.t][log.adl.i] = log.adl.h

        for (let n in currentAnalysis.logHash) {
          if (_.has(currentAnalysis.logHash[n], log.adl.t + '.' + log.adl.i) && currentAnalysis.logHash[n][log.adl.t][log.adl.i] !== log.adl.h) {
            sails.log.warn(logHeader + 'Log matching violation, term: ' + log.adl.t + ', idx: ' + log.adl.i)
            sails.log.debug(logHeader + 'Expected (' + n + '): ' + currentAnalysis.logHash[n][log.adl.t][log.adl.i])
            sails.log.debug(logHeader + 'Found (' + log.n + '): ' + log.adl.h)
            currentAnalysis.violations.logMatching.push({
              lastHash: currentAnalysis.logHash[log.n][log.adl.t][log.adl.i],
              log: log
            })
          }
        }
      }

      // Leader append only check
      if (log.rl) {
        let newLogHash = {}
        for (let t in currentAnalysis.logHash[log.n]) {
          newLogHash[t] = {}
          for (let i in currentAnalysis.logHash[log.n][t]) {
            if (i < log.rl.si || i > log.rl.ei) newLogHash[t][i] = currentAnalysis.logHash[log.n][t][i]
          }
        }
        currentAnalysis.logHash[log.n] = newLogHash

        if (currentAnalysis.nodeStatus[log.n] === 'Leader') {
        sails.log.warn(logHeader + 'Leader append only violation')
        currentAnalysis.violations.leaderAppendOnly.push({
          nodeStatus: currentAnalysis.nodeStatus[log.n],
          log: log
        })}
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