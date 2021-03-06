let sails

const logHeader = '[Hooks][Log] '

const colorNew = '#52c5aa'
const colorCandidate = '#1266f1'
const colorLeader = '#b23cfd'
const colorFollower = '#39c0ed'
const colorShutdown = '#f93154'
const black = '#000000'
const lastLogslimit = 20

let currentLogIdx = -1
let currentCollection = ''
let currentCollectionCount = -1
let currentState = {
  nodes: [],
  edges: [],
  logs: []
}

function getCurrentCollection() {
  return currentCollection
}

function getCurrentCollectionCount() {
  return currentCollectionCount
}

async function setCurrentCollection(newCollection) {
  if (newCollection !== '') {
    currentCollection = newCollection
    currentLogIdx = 0
    currentState = {
      nodes: [],
      eges: [],
      logs: []
    }
    currentCollectionCount = await sails.getDatastore().manager.collection(currentCollection).count()
    await addLogToState(0)
    await loadLastLogs(0)
    return {
      count: currentCollectionCount,
      state: currentState
    }
  }
}

async function addLogToState(idx) {
  sails.log.debug(logHeader + 'Add log to state: ' + idx)
  const log = await sails.getDatastore().manager.collection(currentCollection).findOne({ i: idx })
  if (log.lu) {
    _.remove(currentState.nodes, node => { return node.id === log.n })
    const newNode = { id: log.n, label: log.n, color: colorNew, state: 'New', lastStateChange: log.i }
    currentState.nodes.push(newNode)
  } else if (log.bf) {
    _.remove(currentState.nodes, node => { return node.id === log.n })
    const newNode = { id: log.n, label: log.n, color: colorFollower, state: 'Follower', lastStateChange: log.i }
    currentState.nodes.push(newNode)
  } else if (log.bc) {
    _.remove(currentState.nodes, node => { return node.id === log.n })
    const newNode = { id: log.n, label: log.n, color: colorCandidate, state: 'Candidate', lastStateChange: log.i }
    currentState.nodes.push(newNode)
  } else if (log.bl) {
    _.remove(currentState.nodes, node => { return node.id === log.n })
    const newNode = { id: log.n, label: log.n, color: colorLeader, state: 'Leader', lastStateChange: log.i }
    currentState.nodes.push(newNode)
  } else if (log.sd) {
    _.remove(currentState.nodes, node => { return node.id === log.n })
    const newNode = { id: log.n, label: log.n, color: colorShutdown, state: 'Shutdown', lastStateChange: log.i }
    currentState.nodes.push(newNode)
  }
}

async function findLastState(node) {
  const logs = await sails.getDatastore().manager.collection(currentCollection)
  .find({
    n: node.id,
    i: {$lt: node.lastStateChange},
    $or: [{lu: {$exists: true}}, {bf: {$exists: true}}, {bc: {$exists: true}}, {bl: {$exists: true}}]
  })
  .sort({ i: -1 })
  .limit(1)
  .toArray()

  const log = logs[0]
  if (log.lu) return 'New'
  if (log.bf) return 'Follower'
  if (log.bc) return 'Candidate'
  if (log.bl) return 'Leader'
  return 'New'
}

function getColorFromState(state) {
  if (state === 'New') return colorNew
  if (state === 'Follower') return colorFollower
  if (state === 'Candidate') return colorCandidate
  if (state === 'Leader') return colorLeader
}

async function removeLogFromState(idx) {
  sails.log.debug(logHeader + 'Remove log from state: ' + idx)
  const log = await sails.getDatastore().manager.collection(currentCollection).findOne({ i: idx })
  if (log.lu) {
    _.remove(currentState.nodes, node => { return node.id === log.n })
  }
  if (log.bf || log.bc || log.bl) {
    const oldNode = _.remove(currentState.nodes, node => { return node.id === log.n })[0]
    const lastState = await findLastState(oldNode)
    const newNode = { id: log.n, label: log.n, color: getColorFromState(lastState), state: lastState, lastStateChange: idx - 1 }
    currentState.nodes.push(newNode)
  }
}

function calcColor(i) {
  if (i === '0') return black + 'ff'
  if (i === '1') return black + '50'
  if (i === '2') return black + '40'
  if (i === '3') return black + '30'
  if (i === '4') return black + '20'
}

async function loadLastLogs(idx) {
  const logs = await sails.getDatastore().manager.collection(currentCollection)
  .find({ i: { $gte: idx - lastLogslimit, $lte: idx } }).sort({i: -1}).toArray()
  currentState.logs = logs
  const newEdges = []
  for (let i in logs.slice(0, 5)) {
    const log = logs[i]
    const colorOpacity = calcColor(i)
    const color = { color: colorOpacity }
    const font = { color: color.color }
    if (i === '0') {
      font.strokeWidth = 2
      font.strokeColor = '#ff0000'
    }
    if (log.ctn && log.n !== log.ctn.i) {
      // Connected
      const newEdge = { from: log.n, to: log.ctn.i, arrows: 'to', idx: i, color, font }
      newEdge.label = 'Connect'

      newEdges.push(newEdge)
    } else if (log.saer) {
      // Send AppendEntriesRPC
      const newEdge = { from: log.n, to: log.saer.i, arrows: 'to', idx: i, color, font }
      newEdge.label = 'Send AppendEntriesRPC'

      newEdges.push(newEdge)
    } else if (log.raer) {
      // Respond to AppendEntriesRPC
      const newEdge = { from: log.n, to: log.raer.i, arrows: 'to', idx: i, color, font }
      if (log.raer.s === 'true') {
        newEdge.label = 'Respond AppendEntriesRPC: True'
      } else {
        newEdge.label = 'Respond AppendEntriesRPC: False'
      }

      newEdges.push(newEdge)
    } else if (log.srvr) {
      // Send RequestVoteRPC
      const newEdge = { from: log.n, to: log.srvr.i, arrows: 'to', idx: i, color, font }
      newEdge.label = 'Send RequestVoteRPC'

      newEdges.push(newEdge)
    } else if (log.rrvr) {
      // Respond to RequestVoteRPC
      const newEdge = { from: log.n, to: log.rrvr.i, arrows: 'to', idx: i, color, font }
      if (log.rrvr.s === 'true') {
        newEdge.label = 'Respond RequestVoteRPC: True'
      } else {
        newEdge.label = 'Respond RequestVoteRPC: False'
      }

      newEdges.push(newEdge)
    } else if (log.isr) {
      // Send InstallSnapshotRPC
      const newEdge = { from: log.n, to: log.isr.i, arrows: 'to', idx: i, color, font }
      newEdge.label = 'Send InstallSnapshotRPC'

      newEdges.push(newEdge)
    } else if (log.risr) {
      // Respond to InstallSnapshotRPC
      const newEdge = { from: log.n, to: log.risr.i, arrows: 'to', idx: i, color, font }
      if (log.risr.s === 'true') {
        newEdge.label = 'Respond InstallSnapshotRPC: True'
      } else {
        newEdge.label = 'Respond InstallSnapshotRPC: False'
      }

      newEdges.push(newEdge)
    }
  }
  newEdges.reverse()
  currentState.edges = newEdges
}

async function simulateLogsAction(newIdx) {
  if (newIdx > currentLogIdx) {
    while (newIdx < currentCollectionCount && newIdx > currentLogIdx) {
      currentLogIdx++
      await addLogToState(currentLogIdx)
    }
    await loadLastLogs(newIdx)
  } else if (newIdx < currentLogIdx) {
    while (newIdx > 0 && newIdx < currentLogIdx) {
      await removeLogFromState(currentLogIdx)
      currentLogIdx--
    }
    await loadLastLogs(newIdx)
  }
  return currentState
}

module.exports = function defineLogsHook (sailsInstance) {
  if (!sails) sails = sailsInstance

  return {
    getCurrentCollection,
    setCurrentCollection,
    simulateLogsAction,
    getCurrentCollectionCount
  }
}