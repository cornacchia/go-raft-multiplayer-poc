import 'bootstrap'
import 'bootstrap/dist/css/bootstrap.min.css';
const _ = require('lodash')
const $ = require('jquery')
const vis = require('vis-network/standalone')

let serverState = {
  collections: [],
  dbState: {
    loadingLogs: false,
    error: ""
  }
}

const clientState = {
  currentCollection: "",
  nOfDocs: 0,
  currentIdx: -1
}

function resetClientState(currentCollection) {
  clientState.currentCollection = currentCollection
  clientState.nOfDocs = 0
  clientState.currentIdx = -1
}

function getDateString(dateString) {
  const date = new Date(dateString)
  return date.getHours() + ':' + date.getMinutes() + ':' + date.getSeconds() + '.' + date.getMilliseconds()
}

function getLogHtml(log) {
  let htmlString = getDateString(log.t) + ' '
  htmlString += '<strong>' + log.n + '</strong> '
  if (log.lu) htmlString += 'Listener up'
  if (log.ctn) {
    htmlString += '<i class="fas fa-arrow-right"></i> <strong>' + log.ctn.i + '</strong> '
    htmlString += 'Connected'
  }
  if (log.saer) {
    htmlString += '<i class="fas fa-arrow-right"></i> <strong>' + log.saer.i + '</strong> '
    htmlString += 'Send AppendEntriesRPC'
  }
  if (log.raer) {
    htmlString += '<i class="fas fa-arrow-right"></i> <strong>' + log.raer.i + '</strong> '
    htmlString += 'Respond to AppendEntriesRPC '
    if (log.raer.s === 'true') htmlString += '<i class="fas fa-check"></i>'
    else htmlString += '<i class="fas fa-times"></i>'
  }
  if (log.srvr) {
    htmlString += '<i class="fas fa-arrow-right"></i> <strong>' + log.srvr.i + '</strong> '
    htmlString += 'Send RequestVoteRPC'
  }
  if (log.rrvr) {
    htmlString += '<i class="fas fa-arrow-right"></i> <strong>' + log.rrvr.i + '</strong> '
    htmlString += 'Respond to RequestVoteRPC'
    if (log.rrvr.s === 'true') htmlString += '<i class="fas fa-check"></i>'
    else htmlString += '<i class="fas fa-times"></i>'
  }
  if (log.isr) {
    htmlString += '<i class="fas fa-arrow-right"></i> <strong>' + log.isr.i + '</strong> '
    htmlString += 'Send InstallSnapshotRPC'
  }
  if (log.risr) {
    htmlString += '<i class="fas fa-arrow-right"></i> <strong>' + log.risr.i + '</strong> '
    htmlString += 'Respond to InstallSnapshotRPC'
    if (log.risr.s === 'true') htmlString += '<i class="fas fa-check"></i>'
    else htmlString += '<i class="fas fa-times"></i>'
  }
  if (log.bf) htmlString += 'Become Follower'
  if (log.bc) htmlString += 'Become Candidate'
  if (log.bl) htmlString += 'Become Leader (term: ' + log.bl + ')'
  if (log.al) htmlString += 'Apply log: ' + log.al
  if (log.sd) htmlString += 'Shutting down'
  return htmlString
}

function renderLogs(state) {
  let htmlString = ''
  for (const log of state.logs) {
    htmlString += '<li class="list-group-item">'
    htmlString += getLogHtml(log)
    htmlString += '</li>\n'
  }
  $('#logList').html(htmlString)
}

function renderNodes(state) {
  let htmlString = ''
  for (const node of state.nodes) {
    htmlString += '<li class="list-group-item">'
    htmlString += node.id
    if (node.state === 'New') htmlString += ' ' + '<span class="badge badge-new">New</span>'
    if (node.state === 'Candidate') htmlString += ' ' + '<span class="badge badge-candidate">Candidate</span>'
    if (node.state === 'Leader') htmlString += ' ' + '<span class="badge badge-leader">Leader</span>'
    htmlString += '</li>\n'
  }
  $('#nodeList').html(htmlString)
}

function renderNetwork(state) {
  const container = document.getElementById("networkView")
  const options = {
    physics: {
      enabled: true,
      solver: "forceAtlas2Based",
      stabilization: {
        enabled: false
      }
    }
  }
  new vis.Network(container, { nodes: state.nodes, edges: state.edges }, options)
}

function renderState(state) {
  renderNetwork(state)
  renderNodes(state)
  renderLogs(state)
  $('#logLoadingMsg').attr('hidden', true)
  $('.logCmd').prop('disabled', false)
}

function fetchState() {
  $('#logLoadingMsg').removeAttr('hidden')
  $('.logCmd').prop('disabled', true)
  $.post('/simulateLog', { idx: clientState.currentIdx })
  .done(renderState)
}

function prevLog() {
  clientState.currentIdx = Math.max(clientState.currentIdx - 1, 0)
  $('#currentIdx').val(clientState.currentIdx)
  fetchState()
}

function nextLog() {
  clientState.currentIdx = Math.min(clientState.currentIdx + 1, clientState.nOfDocs)
  $('#currentIdx').val(clientState.currentIdx)
  fetchState()
}

function setNewIdx() {
  const val = parseInt($('#currentIdx').val())
  if (val >= 0 && val < clientState.nOfDocs) {
    clientState.currentIdx = val
    if (clientState.currentCollection !== '') fetchState()
  } else {
    $('#currentIdx').val(clientState.currentIdx)
  }
}

function loadAndAnalyzeLogs() {
  $('#loadAndAnalyzeLogs').prop('disabled', true)
  $('#raft-log-folder').prop('disabled', true)
  $('#db-collection').prop('disabled', true)
  $('#collectionSelect').prop('disabled', true)
  $('#loadingMsg').removeAttr('hidden')
  $('#errorMsg').attr('hidden', true)
  $('#okMsg').attr('hidden', true)
  const raftLogFolder = $('#raft-log-folder').val()
  const collectionName = $('#db-collection').val()
  $.post('/loadLogs', {raftLogFolder, collectionName})
}

function updateCollection(res) {
  clientState.nOfDocs = res.count
  clientState.currentIdx = 0
  $('#nOfDocs').text(res.count)
  $('#currentIdx').val(clientState.currentIdx)
  renderState(res.state)
}

function setNewCollection(evt) {
  resetClientState($(evt.target).val())
  renderCollections(serverState.collections)
  $.post('/setCollection', { collection: clientState.currentCollection })
  .done(updateCollection)
}

function renderCollections(collections) {
  let htmlString = ''
  if (clientState.currentCollection !== '') {
    htmlString += '<option selected>' + clientState.currentCollection + '</option>\n'
  } else {
    htmlString += '<option selected>Select a collection to see its logs</option>\n'
  }
  for (let collection of collections) {
    if (collection !== clientState.currentCollection ) htmlString += '<option value="' + collection + '">' + collection + '</option>\n'
  }
  $('#collectionSelect').html(htmlString)
}

function renderLoadingLogs(loadingLogs) {
  if (loadingLogs) {
    $('#loadingMsg').removeAttr('hidden')
  } else {
    $('#loadingMsg').attr('hidden', true)
    $('#loadAndAnalyzeLogs').prop('disabled', false)
    $('#raft-log-folder').prop('disabled', false)
    $('#db-collection').prop('disabled', false)
    $('#collectionSelect').prop('disabled', false)
    if (serverState.dbState.error === "") $('#okMsg').removeAttr('hidden')
  }
}

function renderError(error) {
  if (error !== '') {
    $('#errorMsg').removeAttr('hidden')
    $('#errorMsg').text(error)
  } else {
    $('#errorMsg').attr('hidden', true)
  }
}

function updateState(state) {
  if (!_.isEqual(serverState.collections, state.collections)) renderCollections(state.collections)
  if (serverState.dbState.loadingLogs != state.dbState.loadingLogs) {
    renderLoadingLogs(state.dbState.loadingLogs)
  }
  if (serverState.dbState.error != state.dbState.error) {
    renderError(state.dbState.error)
  }
  serverState = state
}

function getState() {
  $.get('/state')
  .done(updateState)
}

function start() {
  $("#loadAndAnalyzeLogs").on('click', loadAndAnalyzeLogs)
  $("#prevLog").on('click', prevLog)
  $("#nextLog").on('click', nextLog)
  $("#collectionSelect").on('change', setNewCollection)
  $("#currentIdx").on('change', setNewIdx)
  resetClientState('')
  getState()
  setInterval(getState, 2000)
}

start()