package raft

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestSetup(t *testing.T) {
	log.SetLevel(log.ErrorLevel)
}

// Tests for RequestVoteRPC

// Reply false if RPC term is less than currentTerm
func TestRVRespFalseIfRPCTermLessThanCurrentTerm(t *testing.T) {
	var state = newState("6666", []ServerID{"6667"}, nil, nil)
	var requestToVote = RequestVoteArgs{5, "6667", 0, 0}
	(*state).currentTerm = 6

	var response = (*state).handleRequestToVote(&requestToVote)
	if response.VoteGranted {
		t.Log("Node should not give vote if a Candidate term is less than its current term")
		t.Fail()
	}
}

// Reply false if a vote was already casted for this election
func TestRVRespFalseIfAlreadyVoted(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var requestToVote = RequestVoteArgs{5, "6667", 3, 3}
	(*state).currentTerm = 4
	(*state).votedFor = "6668"

	var response = (*state).handleRequestToVote(&requestToVote)
	if response.VoteGranted {
		t.Log("Node should not vote for more than a Candidate for a term election")
		t.Fail()
	}
}

// Reply false if a Candidate's log is not as up to date as the receiver's log
func TestRVRespFalseIfCandidateLogOutOfDate(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var requestToVote = RequestVoteArgs{5, "6667", 1, 1}
	(*state).currentTerm = 4
	(*state).addNewNoopLog()
	(*state).addNewNoopLog()
	(*state).addNewNoopLog()

	var response = (*state).handleRequestToVote(&requestToVote)
	if response.VoteGranted {
		t.Log("Node should not vote for a Candidate with an out of date log")
		t.Fail()
	}
}

// Reply true in votedFor null and Candidate's log as up to date as receiver's log
func TestRVRespTrueIfCandidateLogUpToDateAndVotedForNull(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var requestToVote = RequestVoteArgs{5, "6667", 1, 1}
	(*state).currentTerm = 1
	(*state).addNewNoopLog() // Idx: 0
	(*state).addNewNoopLog() // Idx: 1

	var response = (*state).handleRequestToVote(&requestToVote)
	if !response.VoteGranted {
		t.Log("Node should vote for a Candidate with an up to date log if no vote was casted for this election")
		t.Fail()
	}
}

// Reply true if already voted for this Candidate and log is up to date
func TestRVRespTrueIfCandidateLogUpToDateAndVotedForCandidate(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var requestToVote = RequestVoteArgs{5, "6667", 1, 1}
	(*state).currentTerm = 1
	(*state).addNewNoopLog() // Idx: 0
	(*state).addNewNoopLog() // Idx: 1
	(*state).votedFor = "6667"

	var response = (*state).handleRequestToVote(&requestToVote)
	if !response.VoteGranted {
		t.Log("Node should vote for a Candidate with an up to date log if it already voted for that Candidate")
		t.Fail()
	}
}

// Tests for AppendEntriesRPC

func createRaftLogNOOPArray(from int, to int, term int) []RaftLog {
	var result = []RaftLog{}
	for idx := from; idx <= to; idx++ {
		var emptyCfgLog = ConfigurationLog{false, "", nil}
		var emptyLog = GameLog{"", -1, "", []byte{}, nil}
		result = append(result, RaftLog{idx, term, Noop, emptyLog, emptyCfgLog})
	}
	return result
}

// Reply false if term < currentTerm
// This applies to all node states
func TestAERespFalseIfTermLessThanCurrentTerm(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var appendEntries = AppendEntriesArgs{4, "6667", 3, 3, createRaftLogNOOPArray(4, 6, 3), 3}
	(*state).currentTerm = 5

	var response = (*state).handleAppendEntries(&appendEntries)
	if response.Success {
		t.Log("Node should not accept AppendEntriesRPC for an outdated term")
		t.Fail()
	}
}

// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
// In this case the term and index are different
func TestAERespFalseIfNoMatchingPrevLogIndexPrevLogTermDifferentTermAndIndex(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var appendEntries = AppendEntriesArgs{4, "6667", 3, 3, createRaftLogNOOPArray(4, 6, 3), 3}
	(*state).currentTerm = 1
	(*state).addNewNoopLog()

	var response = (*state).handleAppendEntries(&appendEntries)
	if response.Success {
		t.Log("Node should not accept AppendEntriesRPC if its log does not contain a matching log for PrevLogIndex and PrevLogTerm")
		t.Fail()
	}
}

// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
// In this case only the term is different
func TestAERespFalseIfNoMatchingPrevLogIndexPrevLogTermDifferentTerm(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var appendEntries = AppendEntriesArgs{4, "6667", 3, 3, createRaftLogNOOPArray(4, 6, 3), 3}
	(*state).currentTerm = 2
	copy((*state).logs[:], createRaftLogNOOPArray(0, 3, 2))
	(*state).nextLogArrayIdx = 4

	var response = (*state).handleAppendEntries(&appendEntries)
	if response.Success {
		t.Log("Node should not accept AppendEntriesRPC if its log does not contain a matching log for PrevLogTerm")
		t.Fail()
	}
}

// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
func TestAEDeleteConflictingEntries(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var appendEntries = AppendEntriesArgs{2, "6667", 1, 1, createRaftLogNOOPArray(2, 4, 2), 3}
	(*state).currentTerm = 1
	copy((*state).logs[:], createRaftLogNOOPArray(0, 3, 1))
	(*state).nextLogArrayIdx = 4

	var response = (*state).handleAppendEntries(&appendEntries)
	if !response.Success {
		t.Log("No reason to refuse this AppendEntriesRPC, term is up to date, prevLogIndex and term present")
		t.Fail()
	}
	if (*state).logs[0].Idx != 0 || (*state).logs[0].Term != 1 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).logs[1].Idx != 1 || (*state).logs[1].Term != 1 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).logs[2].Idx != 2 || (*state).logs[2].Term != 2 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).logs[3].Idx != 3 || (*state).logs[3].Term != 2 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).logs[4].Idx != 4 || (*state).logs[4].Term != 2 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).nextLogArrayIdx != 5 {
		t.Log("Array index was not updated correctly")
		t.Fail()
	}
}

// Check that a snapshot is taken if there is not enough space for the new logs
func TestAETakeSnapshot(t *testing.T) {
	var snapshotRequestChan = make(chan bool)
	var snapshotResponseChan = make(chan []byte)
	var state = newState("6666", []ServerID{"6667", "6668"}, snapshotRequestChan, snapshotResponseChan)
	var appendEntries = AppendEntriesArgs{2, "6667", 1022, 2, createRaftLogNOOPArray(1023, 1025, 2), 1022}
	(*state).currentTerm = 2
	copy((*state).logs[:], createRaftLogNOOPArray(0, 1022, 2))
	(*state).lastApplied = 1020
	(*state).nextLogArrayIdx = 1023

	go func() {
		<-snapshotRequestChan
		snapshotResponseChan <- []byte{}
	}()

	var response = (*state).handleAppendEntries(&appendEntries)
	if !response.Success {
		t.Log("No reason to refuse this AppendEntriesRPC, term is up to date, prevLogIndex and term present")
		t.Fail()
	}
	if (*state).lastSnapshot == nil {
		t.Log("Node should have taken a snapshot")
		t.Fail()
	} else {
		if (*state).lastSnapshot.lastIncludedIndex != 1020 {
			t.Log("Unexpected last snapshot index")
			t.Fail()
		}
		if (*state).lastSnapshot.lastIncludedTerm != 2 {
			t.Log("Unexpected last snapshot term")
			t.Fail()
		}
	}
	if (*state).logs[0].Idx != 1021 || (*state).logs[0].Term != 2 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).logs[1].Idx != 1022 || (*state).logs[1].Term != 2 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).logs[2].Idx != 1023 || (*state).logs[2].Term != 2 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).logs[3].Idx != 1024 || (*state).logs[2].Term != 2 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
	if (*state).logs[4].Idx != 1025 || (*state).logs[2].Term != 2 {
		t.Log("Unexpected log term or index")
		t.Fail()
	}
}

// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func TestAECommitIndexUpdateMinLeaderCommit(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var appendEntries = AppendEntriesArgs{2, "6667", 1, 1, createRaftLogNOOPArray(2, 4, 2), 3}
	(*state).currentTerm = 1
	copy((*state).logs[:], createRaftLogNOOPArray(0, 3, 1))
	(*state).nextLogArrayIdx = 4
	(*state).commitIndex = 2

	var response = (*state).handleAppendEntries(&appendEntries)
	if !response.Success {
		t.Log("No reason to refuse this AppendEntriesRPC, term is up to date, prevLogIndex and term present")
		t.Fail()
	}
	if (*state).commitIndex != 3 {
		t.Log("Node should have updated its commit index")
		t.Fail()
	}
}

func TestAECommitIndexUpdateMinLastLog(t *testing.T) {
	var state = newState("6666", []ServerID{"6667", "6668"}, nil, nil)
	var appendEntries = AppendEntriesArgs{2, "6667", 1, 1, createRaftLogNOOPArray(2, 4, 2), 6}
	(*state).currentTerm = 1
	copy((*state).logs[:], createRaftLogNOOPArray(0, 3, 1))
	(*state).nextLogArrayIdx = 4
	(*state).commitIndex = 2

	var response = (*state).handleAppendEntries(&appendEntries)
	if !response.Success {
		t.Log("No reason to refuse this AppendEntriesRPC, term is up to date, prevLogIndex and term present")
		t.Fail()
	}
	if (*state).commitIndex != 4 {
		t.Log("Node should have updated its commit index")
		t.Fail()
	}
}
