import 'bootstrap'
import 'bootstrap/dist/css/bootstrap.min.css';
const _ = require('lodash')
const $ = require('jquery')
const vis = require('vis-network/standalone')

let playInterval

let serverState = {
  collections: [],
  dbState: {
    loadingLogs: false,
    error: ""
  },
  analyze: {
    runningAnalysis: false
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
  if (log.bc) htmlString += 'Become Candidate (term: ' + log.bc.t + ')'
  if (log.bl) htmlString += 'Become Leader (term: ' + log.bl.t + ')'
  if (log.al) htmlString += 'Apply log: ' + log.al.i
  if (log.sd) htmlString += 'Shutting down'
  if (log.adl) htmlString += 'Add log: ' + log.adl.i
  if (log.rl) htmlString += 'Remove logs (from: ' + log.rl.si + ', to: ' + log.rl.ei + ')'
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
  if (_.isNil(playInterval)) $('.logCmd.manual').prop('disabled', false)
}

function renderAnalysisGeneralInformation(analysis) {
  let resultHtml = '<strong>General information</strong><br/>'
  resultHtml += '<strong> Last committed log</strong>: ' + analysis.lastCommittedLog + '<br/>'
  resultHtml += '<strong> Last leader term</strong>: ' + analysis.lastLeaderTerm
  $('#generalInformation').html(resultHtml)
}

function renderElectionSafetyViolations(data) {
  if (data.length === 0) return '<li><i class="fas fa-check"></i> No election safety violations</li>'
  let result = ''
  for (const datum of data) {
    result += '<li> Election safety violation. Last leader term: ' + datum.lastLeaderTerm + '; New leader term: ' + datum.log.t + ';</li>'
  }
  return result
}

function renderLeaderAppendOnlyViolations(data) {
  if (data.length === 0) return '<li><i class="fas fa-check"></i> No leader append only violations</li>'
  let result = ''
  for (const datum of data) {
    result += '<li> Leader append only violation. Leader removed ' + datum.log.rl.ei - datum.log.rl.si + ' logs from its list</li>'
  }
  return result
}

function renderLogMatchingViolations(data) {
  if (data.length === 0) return '<li><i class="fas fa-check"></i> No log matching violations</li>'
  let result = ''
  for (const datum of data) {
    result += '<li> Log matching violation. Expected: ' + datum.lastHash + ', found: ' + datum.log.adl.h + '</li>'
  }
  return result
}

function renderStateMachineSafetyViolations(data) {
  if (data.length === 0) return '<li><i class="fas fa-check"></i> No state machine safety violations</li>'
  let result = ''
  for (const datum of data) {
    result += '<li> State machine safety violation. Older log: ' + datum.oldLog + '; Newer log: ' + datum.log + ';</li>'
  }
  return result
}

function renderLeaderCompletenessViolations(data) {
  if (data.length === 0) return '<li><i class="fas fa-check"></i> No leader completeness violations</li>'
  let result = ''
  for (const datum of data) {
    result += '<li> Leader completeness violation. Last committed log: ' + datum.lastCommittedLog + '; Last leader log: ' + datum.lastLeaderLog + ';</li>'
  }
  return result
}

function renderAnalysisResults(analysis) {
  let resultHtml = '<strong>Analysis results</strong><br/>'
  resultHtml += '<ul>'
  resultHtml += renderElectionSafetyViolations(analysis.violations.electionSafety)
  resultHtml += renderLeaderAppendOnlyViolations(analysis.violations.leaderAppendOnly)
  resultHtml += renderLogMatchingViolations(analysis.violations.logMatching)
  resultHtml += renderStateMachineSafetyViolations(analysis.violations.stateMachineSafety)
  resultHtml += renderLeaderCompletenessViolations(analysis.violations.leaderCompleteness)
  resultHtml += '</ul>'
  $('#generalInformation').html(resultHtml)
}

function renderAnalysisMainEvents(analysis) {
  let resultHtml = '<strong>Main Raft events</strong><br/>'
  resultHtml += '<ul>'
  for (const evt of analysis.mainEvents) {
    resultHtml += '<li>'
    resultHtml += '<strong>' + evt.n + '</strong> - '
    resultHtml += getDateString(evt.t) + ' - '
    if (evt.lu) resultHtml += 'Listener up'
    if (evt.bc) resultHtml += 'Become Candidate (term: ' + evt.bc.t + ')'
    if (evt.bl) resultHtml += 'Become Leader (term: ' + evt.bl.t + ')'
    if (evt.sd) resultHtml += 'Shutting down'
    resultHtml += ' - (' + evt.i + ')'
    resultHtml += '</li>'
  }
  resultHtml += '</ul>'
  $('#mainEvents').html(resultHtml)
}

function renderCurrentAnalysis(analysis) {
  renderAnalysisGeneralInformation(analysis)
  renderAnalysisResults(analysis)
  renderAnalysisMainEvents(analysis)
}

function renderAnalyze(analyze) {
  if (!analyze.runningAnalysis) {
    $('#analyzeMsg').attr('hidden', true)
    if (analyze.currentAnalysis.lastLeaderTerm >= 0) {
      renderCurrentAnalysis(analyze.currentAnalysis)
    }
  }
}

function fetchState() {
  $('#logLoadingMsg').removeAttr('hidden')
  if (_.isNil(playInterval)) $('.logCmd.manual').prop('disabled', true)
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

function loadLogs() {
  $('#loadLogs').prop('disabled', true)
  $('#analyzeLogs').prop('disabled', true)
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

function analyzeLogs() {
  $('#analyzeLogs').prop('disabled', true)
  $('#analyzeMsg').removeAttr('hidden')
  $.post('/analyzeLogs')
}

function updateCollection(res) {
  $('.logCmd').prop('disabled', false)
  clientState.nOfDocs = res.count
  clientState.currentIdx = 0
  $('#nOfDocs').text(res.count)
  $('#currentIdx').val(clientState.currentIdx)
  renderState(res.state)
  $('#analyzeLogs').prop('disabled', false)
  $('#generalInformation').html('')
  $('#analysisResults').html('')
  $('#mainEvents').html('')
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
    $('#loadLogs').prop('disabled', false)
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
  if (serverState.dbState.loadingLogs !== state.dbState.loadingLogs) {
    renderLoadingLogs(state.dbState.loadingLogs)
  }
  if (serverState.dbState.error !== state.dbState.error) {
    renderError(state.dbState.error)
  }
  if (serverState.analyze.runningAnalysis !== state.analyze.runningAnalysis) {
    renderAnalyze(state.analyze)
  }
  serverState = state
}

function getState() {
  $.get('/state')
  .done(updateState)
}

function autoPlay() {
  playInterval = window.setInterval(nextLog, 500)
  $('.logCmd.manual').prop('disabled', true)
}

function stopAutoPlay() {
  window.clearInterval(playInterval)
  $('.logCmd.manual').prop('disabled', false)
}

function start() {
  $("#loadLogs").on('click', loadLogs)
  $('#analyzeLogs').on('click', analyzeLogs)
  $("#prevLog").on('click', prevLog)
  $("#nextLog").on('click', nextLog)
  $('#play').on('click', autoPlay)
  $('#stop').on('click', stopAutoPlay)
  $("#collectionSelect").on('change', setNewCollection)
  $("#currentIdx").on('change', setNewIdx)
  resetClientState('')
  getState()
  setInterval(getState, 2000)
}

start()