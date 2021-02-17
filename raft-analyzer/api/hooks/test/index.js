const _ = require('lodash')
const childProcess = require('child_process')

const logHeader = '[Hooks][Test] '

let sails

let testStatistics = {
  testsRun: {
    normal: 0,
    faulty: 0,
    dynamic: 0,
    go_skeletons: 0,
    go_wanderer: 0
  },
  ok: 0,
  violations: {
    electionSafety: 0,
    stateMachineSafety: 0,
    leaderCompleteness: 0,
    leaderAppendOnly: 0,
    logMatching: 0
  }
}
let testStop = false

const modes = ['normal', 'dynamic', 'faulty']
const commands = ['go_skeletons', 'go_wanderer']
const minTime = 60
const maxTime = 120
const normalMinRange = 10
const normalMaxRange = 30
const dynamicMinRange = 20
const dynamicMaxRange = 60
const collectionName = 'testLogs'

function createRandomTest () {
  const mode = modes[_.random(modes.length - 1)]
  const cmd = commands[_.random(commands.length - 1)]
  const time = _.random(minTime, maxTime)
  let range
  if (mode === 'normal') {
    range = _.random(normalMinRange, normalMaxRange)
  } else {
    range = _.random(dynamicMinRange, dynamicMaxRange)
  }

  const command = 'go run main.go ' + mode + ' ' + cmd + ' 1 ' + time + ' ' + range + ' ' + range + ' ' + 10
  return {
    cmd,
    mode,
    command
  }
}

function checkIfTestOK(results) {
  if (results.currentAnalysis.violations.electionSafety > 0) return "electionSafety"
  if (results.currentAnalysis.violations.stateMachineSafety > 0) return "stateMachineSafety"
  if (results.currentAnalysis.violations.leaderCompleteness > 0) return "leaderCompleteness"
  if (results.currentAnalysis.violations.leaderAppendOnly > 0) return "leaderAppendOnly"
  if (results.currentAnalysis.violations.logMatching > 0) return "logMatching"
  return ""
}

async function runTests() {
  while (!testStop) {
    const newTest = createRandomTest()
    const newCommand = newTest.command
    sails.log.debug(logHeader + 'Running: ' + newCommand)
    try {
      childProcess.execSync(newCommand, { cwd: '../go_test', stdio: 'ignore' })
    } catch (err) {
      sails.log.error(logHeader + 'Error running command: ' + newCommand, err)
      continue
    }

    await sails.hooks.db.loadLogs('/tmp', collectionName)
    await sails.hooks.analyze.setCurrentCollection(collectionName)
    await sails.hooks.analyze.runAnalysis()
    const results = sails.hooks.analyze.getState()
    testStatistics.testsRun[newTest.mode]++
    testStatistics.testsRun[newTest.cmd]++
    const testOK = checkIfTestOK(results)
    if (testOK === "") testStatistics.ok++
    else testStatistics.violations[testOK]++

    sails.log.debug('Test last committed log: ' + results.currentAnalysis.lastCommittedLog)
    sails.log.debug('Test last leader term: ' + results.currentAnalysis.lastLeaderTerm)
    sails.log.debug(testStatistics)
  }
  sails.log.debug(logHeader + 'Done')
}

function startTests() {
  runTests()
}

function stopTests() {
  sails.log.debug(logHeader + 'Stop tests after the current one completes')
  testStop = true
}

module.exports = function defineTestHook (sailsInstance) {
  if (!sails) sails = sailsInstance

  return {
    startTests,
    stopTests
  }
}