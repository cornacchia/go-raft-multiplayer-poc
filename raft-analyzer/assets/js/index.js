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

function renderLogs(state) {
  let htmlString = ''
  for (const log of state.logs) {
    htmlString += '<li class="list-group-item">'
    htmlString += log.i
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
}

function fetchState() {
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