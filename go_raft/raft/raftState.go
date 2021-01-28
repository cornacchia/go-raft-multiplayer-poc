package raft

import (
	"fmt"
	"go_raft/engine"
	"math"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const maxElectionTimeout = 300
const minElectionTimeout = 150
const logArrayCapacity = 1024

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

type snapshot struct {
	gameState         *engine.GameState
	lastIncludedIndex int
	lastIncludedTerm  int
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
	currentTerm        int
	votedFor           ServerID
	logs               [logArrayCapacity]RaftLog
	currentLogArrayIdx int
	lastSnapshot       *snapshot
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
	lock                  *sync.Mutex
	snapshotRequestChan   chan bool
	snapshotResponseChan  chan engine.GameState
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
func newState(id string, otherStates []ServerID, snapshotRequestChan chan bool, snapshotResponseChan chan engine.GameState) *stateImpl {
	var lastSentLogIndex = make(map[ServerID]int)
	var nextIndex = make(map[ServerID]int)
	var matchIndex = make(map[ServerID]int)
	var serverConfiguration = make(map[ServerID][2]bool)
	var newServerResponseChan = make(map[ServerID]chan bool)
	var lock = &sync.Mutex{}
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
		[logArrayCapacity]RaftLog{newGameRaftLog(0, 0, engine.GameLog{-1, -1, nil})},
		0,
		nil,
		Follower,
		0,
		0,
		nextIndex,
		matchIndex,
		serverConfiguration,
		0,
		0,
		newServerResponseChan,
		lock,
		snapshotRequestChan,
		snapshotResponseChan}
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
	updateElection(*RequestVoteResponse, bool, bool) (int, int)
	winElection()
	getAppendEntriesArgs(ServerID) *AppendEntriesArgs
	handleAppendEntriesResponse(*AppendEntriesResponse) (int, bool)
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
	removeServer(ServerID)
	updateServerConfiguration(ServerID, [2]bool)
	updateNewServerResponseChans(ServerID, chan bool)
	removeNewServerResponseChan(ServerID)
	getNewServerResponseChan(ServerID) chan bool
	prepareInstallSnapshotRPC() *InstallSnapshotArgs
	handleInstallSnapshotResponse(*InstallSnapshotResponse)
	handleInstallSnapshotRequest(*InstallSnapshotArgs) *InstallSnapshotResponse
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
	var lastLog = _state.logs[_state.currentLogArrayIdx]
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
	var lastLog = _state.logs[_state.currentLogArrayIdx]
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
	_state.lock.Lock()
	log.Debug("Become Leader")
	var lastLog = _state.logs[_state.currentLogArrayIdx]
	_state.currentElectionVotesOld = 0
	_state.currentElectionVotesNew = 0
	_state.currentState = Leader
	_state.currentLeader = _state.id
	for id := range _state.nextIndex {
		_state.nextIndex[id] = lastLog.Idx + 1
	}
	_state.matchIndex[_state.id] = lastLog.Idx
	_state.lastSentLogIndex[_state.id] = lastLog.Idx
	_state.lock.Unlock()
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

func (_state *stateImpl) getAppendEntriesArgs(id ServerID) *AppendEntriesArgs {
	_state.lock.Lock()
	var serverNextIdx = _state.nextIndex[id]
	var logsToSend = _state.logs[serverNextIdx:]
	var lastLogIdx = 0
	var lastLogTerm = 0
	if serverNextIdx > 0 {
		var _, lastLogIdx = _state.findArrayIndexByLogIndex(serverNextIdx - 1)
		var lastLog = _state.logs[lastLogIdx]
		lastLogIdx = lastLog.Idx
		lastLogTerm = lastLog.Term
	}
	// Keep track of the last log actually sent to a follower
	if len(logsToSend) > 0 {
		_state.lastSentLogIndex[id] = logsToSend[len(logsToSend)-1].Idx
	} else {
		_state.lastSentLogIndex[id] = serverNextIdx - 1
	}
	_state.lock.Unlock()
	return _state.prepareAppendEntriesArgs(lastLogIdx, lastLogTerm, logsToSend)
}

func (_state *stateImpl) addNewGameLog(msg engine.GameLog) {
	_state.lock.Lock()
	var lastLog = _state.logs[_state.currentLogArrayIdx]
	if _state.currentLogArrayIdx >= logArrayCapacity-1 {
		_state.takeSnapshot()
	}
	var newLog = newGameRaftLog(lastLog.Idx+1, _state.currentTerm, msg)
	_state.currentLogArrayIdx++
	_state.logs[_state.currentLogArrayIdx] = newLog
	_state.matchIndex[_state.id] = newLog.Idx
	_state.lastSentLogIndex[_state.id] = newLog.Idx
	_state.nextIndex[_state.id] = newLog.Idx + 1
	_state.lock.Unlock()
}

func (_state *stateImpl) addNewConfigurationLog(conf ConfigurationLog) {
	_state.lock.Lock()
	var lastLog = _state.logs[_state.currentLogArrayIdx]
	if _state.currentLogArrayIdx >= logArrayCapacity-1 {
		_state.takeSnapshot()
	}
	var newLog = newConfigurationRaftLog(lastLog.Idx+1, _state.currentTerm, conf)
	_state.currentLogArrayIdx++
	_state.logs[_state.currentLogArrayIdx] = newLog
	_state.matchIndex[_state.id] = newLog.Idx
	_state.lastSentLogIndex[_state.id] = newLog.Idx
	_state.nextIndex[_state.id] = newLog.Idx + 1
	_state.lock.Unlock()
}

func (_state *stateImpl) findArrayIndexByLogIndex(idx int) (bool, int) {
	var found = false
	var result = -1
	for i := _state.currentLogArrayIdx; i >= 0 && !found; i-- {
		if _state.logs[i].Idx == idx {
			found = true
			result = i
		}
	}
	return found, result
}

func (_state *stateImpl) handleAppendEntries(aea *AppendEntriesArgs) *AppendEntriesResponse {
	// Handle Candidate and Leader mode particular conditions
	if _state.currentState != Follower {
		if (*aea).Term >= _state.currentTerm {
			// If AppendEntries RPC received from new leader: convert to follower
			_state.stopElectionTimeout()
			log.Debug("Become Follower")
			_state.currentState = Follower
		} else {
			return &AppendEntriesResponse{_state.id, _state.currentTerm, false}
		}
	}

	// 1. Reply false if rpc term < currentTerm
	if (*aea).Term < _state.currentTerm {
		return &AppendEntriesResponse{_state.id, _state.currentTerm, false}
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	var found, prevLogIdx = _state.findArrayIndexByLogIndex((*aea).PrevLogIndex)
	if !found {
		if _state.lastSnapshot == nil || (*_state.lastSnapshot).lastIncludedTerm != (*aea).PrevLogTerm {
			return &AppendEntriesResponse{_state.id, _state.currentTerm, false}
		}
	} else {
		if _state.logs[prevLogIdx].Term != (*aea).PrevLogTerm {
			return &AppendEntriesResponse{_state.id, _state.currentTerm, false}
		}
	}

	// At this point we can say that if the append entries request is empty
	// then it is an heartbeat an so we can keep _state.currentLeader updated
	_state.currentLeader = (*aea).LeaderID

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	var nextIdx = prevLogIdx + 1
	for i := 0; i < len((*aea).Entries); i++ {
		nextIdx = nextIdx + i
		if nextIdx > _state.currentLogArrayIdx {
			_state.currentLogArrayIdx = nextIdx
			_state.logs[_state.currentLogArrayIdx] = (*aea).Entries[i]
		} else {
			// If the terms conflict remove all the remaining logs
			if _state.logs[nextIdx].Term != (*aea).Entries[i].Term {
				_state.currentLogArrayIdx = nextIdx
				_state.logs[_state.currentLogArrayIdx] = (*aea).Entries[i]
			}
		}
		if _state.currentLogArrayIdx >= logArrayCapacity-1 {
			_state.takeSnapshot()
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	var lastLog = (*aea).Entries[len((*aea).Entries)-1]
	if (*aea).LeaderCommit > _state.commitIndex {
		_state.commitIndex = int(math.Min(float64((*aea).LeaderCommit), float64(lastLog.Idx)))
	}

	return &AppendEntriesResponse{_state.id, _state.currentTerm, true}
}

func (_state *stateImpl) handleAppendEntriesResponse(aer *AppendEntriesResponse) (int, bool) {
	_state.lock.Lock()
	if !(*aer).Success && _state.nextIndex[(*aer).Id] > 0 {
		var snapshot = false
		_state.nextIndex[(*aer).Id]--
		if _state.lastSnapshot != nil && _state.nextIndex[(*aer).Id] <= (*_state.lastSnapshot).lastIncludedIndex {
			snapshot = true
		}
		return -1, snapshot
	}
	_state.nextIndex[(*aer).Id] = _state.lastSentLogIndex[(*aer).Id] + 1
	_state.matchIndex[(*aer).Id] = _state.lastSentLogIndex[(*aer).Id]
	_state.lock.Unlock()
	return _state.matchIndex[(*aer).Id], false
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
	var _, logIdx = _state.findArrayIndexByLogIndex(i)
	return _state.logs[logIdx]
}

func checkMajority(_state *stateImpl, newCount int, oldCount int) bool {
	var oldMajority = true
	var newMajority = newCount > ((*_state).newServerCount)/2
	if (*_state).oldServerCount > 0 {
		oldMajority = oldCount > ((*_state).oldServerCount)/2
	}
	log.Trace(fmt.Sprintf("State check majority: new %d/%d, old %d/%d", newCount, (*_state).newServerCount, oldCount, (*_state).oldServerCount))
	return newMajority && oldMajority
}

func (_state *stateImpl) checkCommits() {
	_state.lock.Lock()
	if len(_state.logs) < 1 {
		return
	}
	var bound = false
	for i := _state.currentLogArrayIdx; !bound && i >= 0; i-- {
		// i > commitIndex
		if _state.logs[i].Idx <= _state.commitIndex {
			bound = true
			continue
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
			if _state.logs[i].Term == _state.currentTerm {
				_state.commitIndex = _state.logs[i].Idx
			}
		}
	}
	_state.lock.Unlock()
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
	_state.lock.Lock()
	_state.lastSentLogIndex[sid] = 0
	_state.nextIndex[sid] = 0
	_state.matchIndex[sid] = 0
	_state.lock.Unlock()
}

func (_state *stateImpl) removeServer(sid ServerID) {
	_state.lock.Lock()
	delete(_state.lastSentLogIndex, sid)
	delete(_state.nextIndex, sid)
	delete(_state.matchIndex, sid)
	if _state.serverConfiguration[sid][0] {
		_state.oldServerCount--
	}
	if _state.serverConfiguration[sid][1] {
		_state.newServerCount--
	}
	delete(_state.serverConfiguration, sid)
	_state.lock.Unlock()
}

func (_state *stateImpl) updateServerConfiguration(sid ServerID, conf [2]bool) {
	_state.lock.Lock()
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
	log.Debug("State: Update server configuration ", _state.serverConfiguration)
	log.Debug(fmt.Sprintf("State: new: %d, old: %d", _state.newServerCount, _state.oldServerCount))
	_state.lock.Unlock()
}

func (_state *stateImpl) updateNewServerResponseChans(id ServerID, channel chan bool) {
	_state.lock.Lock()
	_state.newServerResponseChan[id] = channel
	_state.lock.Unlock()
}

func (_state *stateImpl) removeNewServerResponseChan(id ServerID) {
	_state.lock.Lock()
	delete(_state.newServerResponseChan, id)
	_state.lock.Unlock()
}

func (_state *stateImpl) getNewServerResponseChan(id ServerID) chan bool {
	return _state.newServerResponseChan[id]
}

func (_state *stateImpl) takeSnapshot() {
	log.Debug("Taking snapshot")
	_state.snapshotRequestChan <- true
	currentGameState := <-_state.snapshotResponseChan
	var _, lastAppliedIdx = _state.findArrayIndexByLogIndex(_state.lastApplied)
	var newSnapshot = snapshot{&currentGameState, _state.logs[lastAppliedIdx].Idx, _state.logs[lastAppliedIdx].Term}
	_state.lastSnapshot = &newSnapshot
	_state.currentLogArrayIdx = 0
}

func (_state *stateImpl) prepareInstallSnapshotRPC() *InstallSnapshotArgs {
	var lastSnapshot = *_state.lastSnapshot
	var newInstallSnapshotArgs = InstallSnapshotArgs{
		_state.currentTerm,
		lastSnapshot.lastIncludedIndex,
		lastSnapshot.lastIncludedTerm,
		*lastSnapshot.gameState}
	return &newInstallSnapshotArgs
}

func (_state *stateImpl) handleInstallSnapshotResponse(isr *InstallSnapshotResponse) {
	if !(*isr).Success && (*isr).Term >= _state.currentTerm {
		log.Debug("Become Follower")
		_state.currentState = Follower
		_state.currentTerm = (*isr).Term
		_state.currentLeader = (*isr).Id
	}
}

func (_state *stateImpl) handleInstallSnapshotRequest(isa *InstallSnapshotArgs) *InstallSnapshotResponse {
	// Respond immediately if rpc term < currentTerm
	if (*isa).Term < _state.currentTerm {
		return &InstallSnapshotResponse{_state.id, _state.currentTerm, false}
	}

	// Apply snapshot
	var newGameState = (*isa).Data
	var newSnapshot = snapshot{&newGameState, (*isa).LastIncludedIndex, (*isa).LastIncludedTerm}
	_state.lastSnapshot = &newSnapshot
	_state.currentLogArrayIdx = 0
	return &InstallSnapshotResponse{_state.id, _state.currentTerm, true}
}
