package raft

import (
	"go_raft/engine"
	"math"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
)

const maxElectionTimeout = 300
const minElectionTimeout = 150

// InstanceState represents the possible state that a raft instance server can be in
type instanceState = int
type LogType = int

const (
	// Follower instances only receive updates from the Leader
	Follower instanceState = 0
	// Candidate instances are expecting to become Leaders
	Candidate instanceState = 1
	// Leader instances handle the replication of logs
	Leader instanceState = 2
)

const (
	Game          LogType = 0
	Configuration LogType = 1
)

type ConfigurationLog struct {
	Id          ServerID
	ConnMap     map[ServerID][2]bool
	OldCount    int
	NewCount    int
	ChanApplied chan bool
}

// RaftLog are logs exchanged by the server instances and applied to the game engine (i.e. the state machine)
type RaftLog struct {
	Idx              int
	Term             int
	Type             LogType
	Log              engine.GameLog
	ConfigurationLog ConfigurationLog
}

// ServerID is the identification code for a raft server
type ServerID string

// StateImpl are structures containing the state for all servers
type stateImpl struct {
	// Implementation state
	id                      ServerID
	electionTimeoutStarted  bool
	electionTimer           *time.Timer
	currentElectionVotesNew int
	currentElectionVotesOld int
	lastSentLogIndex        map[ServerID]int
	currentLeader           ServerID
	// Persistent state
	currentTerm int
	votedFor    ServerID
	logs        []RaftLog
	// Volatile state
	currentState instanceState
	commitIndex  int
	lastApplied  int
	// These next entries are for leaders only
	nextIndex             map[ServerID]int
	matchIndex            map[ServerID]int
	serverConfiguration   map[ServerID][2]bool
	newServerCount        int
	oldServerCount        int
	newServerResponseChan map[ServerID]chan bool
}

func newGameRaftLog(idx int, term int, log engine.GameLog) RaftLog {
	var emptyCfgLog = ConfigurationLog{"", nil, 0, 0, nil}
	return RaftLog{idx, term, Game, log, emptyCfgLog}
}

func newConfigurationRaftLog(idx int, term int, cfgLog ConfigurationLog) RaftLog {
	var emptyLog = engine.GameLog{-1, -1, nil}
	return RaftLog{idx, term, Configuration, emptyLog, cfgLog}
}

// NewState returns an empty state, only used once at the beginning
func newState(id string, otherStates []ServerID) *stateImpl {
	var lastSentLogIndex = make(map[ServerID]int)
	var nextIndex = make(map[ServerID]int)
	var matchIndex = make(map[ServerID]int)
	var serverConfiguration = make(map[ServerID][2]bool)
	var newServerResponseChan = make(map[ServerID]chan bool)
	return &stateImpl{
		ServerID(id),
		false,
		nil,
		0,
		0,
		lastSentLogIndex,
		"",
		0,
		"",
		[]RaftLog{newGameRaftLog(0, 0, engine.GameLog{-1, -1, nil})},
		Follower,
		0,
		0,
		nextIndex,
		matchIndex,
		serverConfiguration,
		0,
		0,
		newServerResponseChan}
}

// TODO probably lock state before writing to it

// State interface for raft server instances
type state interface {
	startElection()
	prepareRequestVoteRPC() *RequestVoteArgs
	getState() instanceState
	checkElectionTimeout() *time.Timer
	stopElectionTimeout()
	handleRequestToVote(*RequestVoteArgs) *RequestVoteResponse
	getElectionTimer() *time.Timer
	updateElection(*RequestVoteResponse, bool, bool) (int, int)
	winElection()
	getAppendEntriesArgs(ServerID) *AppendEntriesArgs
	prepareHearthBeat(ServerID) *AppendEntriesArgs
	handleAppendEntriesResponse(*AppendEntriesResponse) int
	addNewGameLog(engine.GameLog)
	addNewConfigurationLog(ConfigurationLog)
	handleAppendEntries(*AppendEntriesArgs) *AppendEntriesResponse
	updateLastApplied() int
	getLog(int) RaftLog
	checkCommits()
	getCurrentLeader() ServerID
	getID() ServerID
	getCommitIndex() int
	addNewServer(ServerID)
	updateServerConfiguration(ServerID, [2]bool)
	updateNewServerResponseChans(ServerID, chan bool)
	removeNewServerResponseChan(ServerID)
	getNewServerResponseChan(ServerID) chan bool
}

/* To start a new election a server:
 * 1. Increment its current term
 * 2. Changes its current state to Candidate
 * 3. Votes for itself
 */
func (_state *stateImpl) startElection() {
	log.Debug("Become Candidate")
	_state.currentTerm++
	_state.currentState = Candidate
	_state.votedFor = _state.id
	// TODO verify that this is correct
	_state.currentElectionVotesNew = 1
	_state.currentElectionVotesOld = 1
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
	// fmt.Println("Stop election timeout")
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
	// fmt.Printf("Handle request to vote %d, %d\n", _state.currentTerm, (*rva).Term)
	var lastLog = _state.logs[len(_state.logs)-1]
	if _state.currentTerm > (*rva).Term {
		return &RequestVoteResponse{_state.id, _state.currentTerm, false}
	} else if (*rva).Term == _state.currentTerm && (_state.votedFor == "" || _state.votedFor == (*rva).CandidateID) && (*rva).LastLogTerm >= lastLog.Term && (*rva).LastLogIndex >= lastLog.Idx {
		// fmt.Println(_state.id, " become Follower")
		_state.stopElectionTimeout()
		log.Debug("Become Follower")
		_state.currentState = Follower
		_state.currentTerm = (*rva).Term
		_state.votedFor = (*rva).CandidateID
		_state.currentLeader = (*rva).CandidateID
		return &RequestVoteResponse{_state.id, _state.currentTerm, true}
	} else if (*rva).Term > _state.currentTerm {
		// Our term is out of date, become follower
		// fmt.Println(_state.id, " become Follower")
		_state.stopElectionTimeout()
		log.Debug("Become Follower")
		_state.currentState = Follower
		_state.currentTerm = (*rva).Term
		_state.votedFor = (*rva).CandidateID
		_state.currentLeader = (*rva).CandidateID
		return &RequestVoteResponse{_state.id, _state.currentTerm, true}
	}

	// fmt.Println("Handle request to vote: default false")
	return &RequestVoteResponse{_state.id, _state.currentTerm, false}
}

func (_state *stateImpl) getElectionTimer() *time.Timer {
	return _state.electionTimer
}

func (_state *stateImpl) updateElection(resp *RequestVoteResponse, old bool, new bool) (int, int) {
	// If the node's current state is stale immediately revert to Follower state
	if (*resp).Term > (_state.currentTerm) {
		// fmt.Println(_state.id, " become Follower")
		_state.stopElectionTimeout()
		_state.currentElectionVotesNew = 0
		_state.currentElectionVotesOld = 0
		_state.currentTerm = (*resp).Term
		log.Debug("Become Follower")
		_state.currentState = Follower
		// Only accept votes for the current term
	} else if (*resp).Term == (_state.currentTerm) && (*resp).VoteGranted == true {
		if old {
			_state.currentElectionVotesOld++
		}
		if new {
			_state.currentElectionVotesNew++
		}
	}
	return _state.currentElectionVotesOld, _state.currentElectionVotesNew
}

func (_state *stateImpl) winElection() {
	log.Debug("Become Leader")
	var lastLog = _state.logs[len(_state.logs)-1]
	_state.currentElectionVotesOld = 0
	_state.currentElectionVotesNew = 0
	_state.currentState = Leader
	_state.currentLeader = _state.id
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

func (_state *stateImpl) addNewGameLog(msg engine.GameLog) {
	var lastLog = _state.logs[len(_state.logs)-1]
	var newLog = newGameRaftLog(lastLog.Idx+1, _state.currentTerm, msg)
	_state.logs = append(_state.logs, newLog)
}

func (_state *stateImpl) addNewConfigurationLog(conf ConfigurationLog) {
	var lastLog = _state.logs[len(_state.logs)-1]
	var newLog = newConfigurationRaftLog(lastLog.Idx+1, _state.currentTerm, conf)
	_state.logs = append(_state.logs, newLog)
}

func (_state *stateImpl) handleAppendEntries(aea *AppendEntriesArgs) *AppendEntriesResponse {
	// Handle Candidate and Leader mode particular conditions
	if _state.currentState != Follower {
		if (*aea).Term >= _state.currentTerm {
			// fmt.Println(_state.id, " become Follower")
			// If AppendEntries RPC received from new leader: convert to follower
			_state.stopElectionTimeout()
			log.Debug("Become Follower")
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
		// fmt.Println("received heartbeat ", (*aea).LeaderID)
		_state.currentLeader = (*aea).LeaderID
	} else {
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
		var lastLog = (*aea).Entries[len((*aea).Entries)-1]
		if (*aea).LeaderCommit > _state.commitIndex {
			_state.commitIndex = int(math.Min(float64((*aea).LeaderCommit), float64(lastLog.Idx)))
		}
	}

	return &AppendEntriesResponse{_state.id, _state.currentTerm, true}
}

func (_state *stateImpl) handleAppendEntriesResponse(aer *AppendEntriesResponse) int {
	// TODO check if follower actually present
	if !(*aer).Success && _state.nextIndex[(*aer).Id] > 0 {
		_state.nextIndex[(*aer).Id]--
		return -1
	}
	_state.nextIndex[(*aer).Id] = _state.lastSentLogIndex[(*aer).Id] + 1
	_state.matchIndex[(*aer).Id] = _state.lastSentLogIndex[(*aer).Id]

	return _state.matchIndex[(*aer).Id]
}

func (_state *stateImpl) updateLastApplied() int {
	// fmt.Println("commit index ", _state.commitIndex)
	if _state.commitIndex > _state.lastApplied {
		_state.lastApplied++
		return _state.lastApplied
	}
	return -1
}

func (_state *stateImpl) getLog(i int) RaftLog {
	return _state.logs[i]
}

func checkMajority(_state *stateImpl, newCount int, oldCount int) bool {
	var oldMajority = true
	var newMajority = newCount >= ((*_state).newServerCount+1)/2
	if (*_state).oldServerCount > 0 {
		oldMajority = oldCount >= ((*_state).oldServerCount+1)/2
	}
	// fmt.Println(newCount, " ", oldCount, " ", (*_state).newServerCount, " ", (*_state).oldServerCount)
	return newMajority && oldMajority
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
		var replicatedFollowersNew = 0
		var replicatedFollowersOld = 0
		for id := range _state.matchIndex {
			conf, _ := _state.serverConfiguration[id]
			if _state.matchIndex[id] >= _state.logs[i].Idx {
				if conf[0] {
					replicatedFollowersOld++
				}
				if conf[1] {
					replicatedFollowersNew++
				}
			}
		}
		if checkMajority(_state, replicatedFollowersNew, replicatedFollowersOld) {
			// log[i].term == currentTerm
			if _state.logs[i].Term == _state.currentTerm {
				_state.commitIndex = _state.logs[i].Idx
			}
		}
	}
}

func (_state *stateImpl) getCurrentLeader() ServerID {
	return _state.currentLeader
}

func (_state *stateImpl) getID() ServerID {
	return _state.id
}

func (_state *stateImpl) getCommitIndex() int {
	return _state.commitIndex
}

func (_state *stateImpl) addNewServer(sid ServerID) {
	_state.lastSentLogIndex[sid] = 0
	_state.nextIndex[sid] = 0
	_state.matchIndex[sid] = 0
}

func (_state *stateImpl) updateServerConfiguration(sid ServerID, conf [2]bool) {
	if sid != _state.id {
		_state.serverConfiguration[sid] = conf
		_state.newServerCount = 0
		_state.oldServerCount = 0
		for _, conf := range _state.serverConfiguration {
			if conf[0] {
				_state.oldServerCount++
			}
			if conf[1] {
				_state.newServerCount++
			}
		}
	}
}

func (_state *stateImpl) updateNewServerResponseChans(id ServerID, channel chan bool) {
	_state.newServerResponseChan[id] = channel
}

func (_state *stateImpl) removeNewServerResponseChan(id ServerID) {
	delete(_state.newServerResponseChan, id)
}

func (_state *stateImpl) getNewServerResponseChan(id ServerID) chan bool {
	return _state.newServerResponseChan[id]
}
