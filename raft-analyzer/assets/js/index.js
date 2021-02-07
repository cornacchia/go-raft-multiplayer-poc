import 'bootstrap'
import 'bootstrap/dist/css/bootstrap.min.css';
import _ from 'lodash'
const $ = require('jquery')

function loadAndAnalyzeLogs() {
  const raftLogFolder = $("#raft-log-folder").val()
  const collectionName = $("#db-collection").val()
  $.post('/loadLogs', {raftLogFolder, collectionName})
}

function start() {
  $("#loadAndAnalyzeLogs").on('click', loadAndAnalyzeLogs)
}

start()