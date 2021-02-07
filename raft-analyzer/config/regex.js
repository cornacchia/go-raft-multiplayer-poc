module.exports.regex = {
  raftLogFile: /go_raft_log_(\d+)/,

  // log regex
  time: /time="(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6})"/,
  connectedToNode: /Raft - Connected to node: (\d+)/, // node id
  sendAppendEntriesRPC: /Sending AppendEntriesRPC: (\d+) (\d+) (\d+)/, // node id, first included index, last included index
  respondAppendEntriesRPC: /Respond to AppendEntriesRPC: (\d+) (.*)/, // leader id, success
  sendRequestVoteRPC: /Sending RequestVoteRPC: (\d+)/, // node id
  respondRequestVoteRPC: /Respond to RequestVoteRPC: (\d+) (.*)/, // candidate id, vote granted
  installSnapshotRPC: /Sending installSnapshotRPC: (\d+) (\d+)/, // node id, last included index
  respondInstallSnapshotRPC: /Respond to InstallSnapshotRPC: (\d+) (.*)/, // node id, success
  becomeFollower: /Become Follower/,
  becomeCandidate: /Become Candidate/,
  becomeLeader: /Become Leader/,
  applyLog: /Raft apply log: (\d+)/,
  shuttingDown: /Shutting down/
}