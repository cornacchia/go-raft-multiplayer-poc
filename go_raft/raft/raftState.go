package raft

import (
	"fmt"
	"go_raft/engine"
	"math"
	"math/rand"
	"time"
)

const maxElectionTimeout = 300
const minElectionTimeout = 150

// InstanceState represents the possible state that a raft instance server can be in
type instanceState = int

const (
	// Follower instances only receive updates from the Leader
	Follower instanceState = 0
	// Candidate instances are expecting to become Leaders
	Candidate instanceState = 1
	// Leader instances handle the replication of logs
	Leader instanceState = 2
)

// PlayerID is the identification code for a player of the game (i.e. a UI instance)
//type PlayerID int

// GameLog implements the structure of raft messages
//type GameLog struct {
//	Action int
//	Id     PlayerID
//}

// RaftLog are logs exchanged by the server instances and applied to the game engine (i.e. the state machine)
type RaftLog struct {
	Idx  int
	Term int
	Log  engine.GameLog
}

// ServerID is the identification code for a raft server
type ServerID string

// StateImpl are structures containing the state for all servers
type stateImpl struct {
	// Implementation state
	id                     ServerID
	electionTimeoutStarted bool
	electionTimer          *time.Timer
	currentElectionVotes   int
	lastSentLogIndex       map[ServerID]int
	currentLeader          ServerID
	// Persistent state
	currentTerm int
	votedFor    ServerID
	logs        []RaftLog
	// Volatile state
	currentState instanceState
	commitIndex  int
	lastApplied  int
	// These next entries are for leaders only
	nextIndex  map[ServerID]int
	matchIndex map[ServerID]int
}

// NewState returns an empty state, only used once at the beginning
func newState(id string, otherStates []ServerID) *stateImpl {
	var lastSentLogIndex = make(map[ServerID]int)
	var nextIndex = make(map[ServerID]int)
	var matchIndex = make(map[ServerID]int)
	for _, id := range otherStates {
		lastSentLogIndex[id] = 0
		nextIndex[id] = 0
		matchIndex[id] = 0
	}
	return &stateImpl{
		ServerID(id),
		false,
		nil,
		0,
		lastSentLogIndex,
		"",
		0,
		"",
		[]RaftLog{{0, 0, engine.GameLog{-1, -1}}},
		Follower,
		0,
		0,
		nextIndex,
		matchIndex}
}

// State interface for raft server instances
type state interface {
	startElection()
	prepareRequestVoteRPC() *RequestVoteArgs
	getState() instanceState
	checkElectionTimeout() *time.Timer
	stopElectionTimeout()
	handleRequestToVote(*RequestVoteArgs) *RequestVoteResponse
	getElectionTimer() *time.Timer
	updateElection(*RequestVoteResponse) int
	winElection()
	getAppendEntriesArgs(ServerID) *AppendEntriesArgs
	prepareHearthBeat(ServerID) *AppendEntriesArgs
	handleAppendEntriesResponse(*AppendEntriesResponse)
	addNewLog(engine.GameLog)
	handleAppendEntries(*AppendEntriesArgs) *AppendEntriesResponse
	updateLastApplied() int
	getLog(int) RaftLog
	checkCommits()
}

/* To start a new election a server:
 * 1. Increment its current term
 * 2. Changes its current state to Candidate
 * 3. Votes for itself
 */
func (_state *stateImpl) startElection() {
	fmt.Println("Start new election")
	_state.currentTerm++
	_state.currentState = Candidate
	_state.votedFor = _state.id
	_state.currentElectionVotes = 1
}

func (_state *stateImpl) prepareRequestVoteRPC() *RequestVoteArgs {
	var lastLog = _state.logs[len(_state.logs)-1]
	return &RequestVoteArgs{_state.currentTerm, _state.id, lastLog.Idx, lastLog.Term}
}

func (_state *stateImpl) getState() instanceState {
	return _state.currentState
}

// Only start a new election timeout if its not already running
func (_state *stateImpl) checkElectionTimeout() *time.Timer {
	if !_state.electionTimeoutStarted {
		// The election timeout is randomized to prevent split votes
		var electionTimeout = time.Duration(time.Millisecond * time.Duration(rand.Intn(maxElectionTimeout-minElectionTimeout)+minElectionTimeout))
		// var electionTimeout = time.Duration(time.Second * 2)
		_state.electionTimeoutStarted = true
		_state.electionTimer = time.NewTimer(electionTimeout)
	}
	return _state.electionTimer
}

func (_state *stateImpl) stopElectionTimeout() {
	fmt.Println("Stop election timeout")
	// Ensure the election timer is actually stopped and the channel empty
	if !_state.electionTimer.Stop() {
		select {
		case <-_state.electionTimer.C:
		default:
		}
	}
	_state.electionTimeoutStarted = false
}

func (_state *stateImpl) handleRequestToVote(rva *RequestVoteArgs) *RequestVoteResponse {
	fmt.Printf("Handle request to vote %d, %d\n", _state.currentTerm, (*rva).Term)
	if _state.currentTerm > (*rva).Term {
		fmt.Println("Handle request to vote: currentTerm greater than request")
		return &RequestVoteResponse{_state.currentTerm, false}
	}
	var lastLog = _state.logs[len(_state.logs)-1]
	/*
		if _state.currentTerm < (*rva).Term {
			fmt.Println("Handle request to vote: accept leader with greater term")
			_state.stopElectionTimeout()
			_state.currentState = Follower
			_state.currentTerm = (*rva).Term
			return &RequestVoteResponse{_state.currentTerm, true}
		}*/
	if (_state.votedFor == "" || _state.votedFor == (*rva).CandidateID) && (*rva).LastLogTerm >= lastLog.Term && (*rva).LastLogIndex >= lastLog.Idx {
		fmt.Println("Accepted new leader: other reasons")
		_state.currentState = Follower
		_state.currentTerm = (*rva).Term
		_state.votedFor = (*rva).CandidateID
		_state.stopElectionTimeout()
		return &RequestVoteResponse{_state.currentTerm, true}
	}
	fmt.Println("Handle request to vote: default false")
	return &RequestVoteResponse{_state.currentTerm, false}
}

func (_state *stateImpl) getElectionTimer() *time.Timer {
	return _state.electionTimer
}

func (_state *stateImpl) updateElection(resp *RequestVoteResponse) int {
	// If the node's current state is stale immediately revert to Follower state
	if (*resp).Term > (_state.currentTerm) {
		_state.stopElectionTimeout()
		_state.currentElectionVotes = 0
		_state.currentTerm = (*resp).Term
		_state.currentState = Follower
		// Only accept votes for the current term
	} else if (*resp).Term == (_state.currentTerm) && (*resp).VoteGranted == true {
		_state.currentElectionVotes++
	}
	return _state.currentElectionVotes
}

func (_state *stateImpl) winElection() {
	var lastLog = _state.logs[len(_state.logs)-1]
	_state.currentElectionVotes = 0
	_state.currentState = Leader
	for id := range _state.nextIndex {
		_state.nextIndex[id] = lastLog.Idx + 1
	}
}

func (_state *stateImpl) prepareAppendEntriesArgs(lastLogIdx int, lastLogTerm int, logsToSend []RaftLog) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		_state.currentTerm,
		_state.id,
		lastLogIdx,
		lastLogTerm,
		logsToSend,
		_state.commitIndex}
}

func (_state *stateImpl) prepareHearthBeat(id ServerID) *AppendEntriesArgs {
	var lastLogIdx = 0
	var lastLogTerm = 0
	if len(_state.logs) > 0 {
		var lastLog = _state.logs[len(_state.logs)-1]
		lastLogIdx = lastLog.Idx
		lastLogTerm = lastLog.Term
	}
	return _state.prepareAppendEntriesArgs(lastLogIdx, lastLogTerm, []RaftLog{})
}

func (_state *stateImpl) getAppendEntriesArgs(id ServerID) *AppendEntriesArgs {
	var serverNextIdx = _state.nextIndex[id]
	var logsToSend = _state.logs[serverNextIdx:]
	var lastLogIdx = 0
	var lastLogTerm = 0
	if serverNextIdx > 0 {
		var lastLog = _state.logs[serverNextIdx-1]
		lastLogIdx = lastLog.Idx
		lastLogTerm = lastLog.Term
	}
	// Keep track of the last log actually sent to a follower
	if len(logsToSend) > 0 {
		_state.lastSentLogIndex[id] = logsToSend[len(logsToSend)-1].Idx
	}
	return _state.prepareAppendEntriesArgs(lastLogIdx, lastLogTerm, logsToSend)
}

func (_state *stateImpl) addNewLog(msg engine.GameLog) {
	var lastLog = _state.logs[len(_state.logs)-1]
	var newLog = RaftLog{lastLog.Idx + 1, _state.currentTerm, msg}
	_state.logs = append(_state.logs, newLog)
}

func (_state *stateImpl) handleAppendEntries(aea *AppendEntriesArgs) *AppendEntriesResponse {
	// Handle Candidate and Leader mode particular conditions
	if _state.currentState != Follower {
		if (*aea).Term >= _state.currentTerm {
			// If AppendEntries RPC received from new leader: convert to follower
			_state.stopElectionTimeout()
			_state.currentState = Follower
		} else {
			return &AppendEntriesResponse{_state.id, _state.currentTerm, false}
		}
	}
	// 1. Reply false if term < currentTerm
	if (*aea).Term < _state.currentTerm {
		return &AppendEntriesResponse{_state.id, _state.currentTerm, false}
	}
	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if _state.logs[(*aea).PrevLogIndex].Term != (*aea).PrevLogTerm {
		return &AppendEntriesResponse{_state.id, _state.currentTerm, false}
	}
	// At this point we can say that if the append entries request is empty
	// then it is an heartbeat an so we can keep _state.currentLeader updated
	if len((*aea).Entries) == 0 {
		_state.currentLeader = (*aea).LeaderID
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	for i := 0; i < len((*aea).Entries); i++ {
		var nextIdx = (*aea).PrevLogIndex + i + 1
		if (nextIdx) >= len(_state.logs) {
			_state.logs = append(_state.logs, (*aea).Entries[i])
		} else {
			// If the terms conflict remove all the remaining logs
			if _state.logs[nextIdx].Term != (*aea).Entries[i].Term {
				_state.logs = _state.logs[:nextIdx]
				_state.logs = append(_state.logs, (*aea).Entries[i])
			}
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	var lastLog = _state.logs[len(_state.logs)-1]
	if (*aea).LeaderCommit > _state.commitIndex {
		_state.commitIndex = int(math.Min(float64((*aea).LeaderCommit), float64(lastLog.Idx)))
	}
	return &AppendEntriesResponse{_state.id, _state.currentTerm, true}
}

func (_state *stateImpl) handleAppendEntriesResponse(aer *AppendEntriesResponse) {
	// TODO check if follower actually present
	if !(*aer).Success {
		_state.nextIndex[(*aer).Id]--
	} else {
		_state.nextIndex[(*aer).Id] = _state.lastSentLogIndex[(*aer).Id] + 1
		_state.matchIndex[(*aer).Id] = _state.lastSentLogIndex[(*aer).Id]
	}
}

func (_state *stateImpl) updateLastApplied() int {
	if _state.commitIndex > _state.lastApplied {
		_state.lastApplied++
		return _state.lastApplied
	}
	return -1
}

func (_state *stateImpl) getLog(i int) RaftLog {
	return _state.logs[i]
}

func (_state *stateImpl) checkCommits() {
	if len(_state.logs) < 1 {
		return
	}
	var bound = false
	for i := len(_state.logs) - 1; !bound && i >= 0; i-- {
		// i > commitIndex
		if _state.logs[i].Idx <= _state.commitIndex+1 {
			bound = true
		}
		// A majority of matchIndex[j] >= i
		var replicatedFollowers = 0
		for id := range _state.matchIndex {
			if _state.matchIndex[id] >= _state.logs[i].Idx {
				replicatedFollowers++
			}
		}
		if replicatedFollowers >= (len(_state.nextIndex)+1)/2 {
			// log[i].term == currentTerm
			if _state.logs[i].Term == _state.currentTerm {
				_state.commitIndex = _state.logs[i].Idx
			}
		}
	}
}
