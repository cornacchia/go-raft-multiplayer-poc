package raft

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"math"

	log "github.com/sirupsen/logrus"
)

const maxElectionTimeout = 600
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
	Noop          LogType = 2
)

type ConfigurationLog struct {
	Add         bool
	Server      ServerID
	ChanApplied chan bool
}

type GameLog struct {
	Id          string
	ActionId    int64
	Type        string
	Action      []byte
	ChanApplied chan bool
}

// RaftLog are logs exchanged by the server instances and applied to the game engine (i.e. the state machine)
type RaftLog struct {
	Idx              int
	Term             int
	Type             LogType
	Log              GameLog
	ConfigurationLog ConfigurationLog
}

type snapshot struct {
	serverConfiguration map[ServerID]bool
	gameState           []byte
	lastIncludedIndex   int
	lastIncludedTerm    int
	hash                [32]byte
}

type appendEntriesSituationStruct struct {
	lastIndex int
	lastHash  [32]byte
}

type clientStateStruct struct {
	lastActionApplied int64
}

// ServerID is the identification code for a raft server
type ServerID string

// StateImpl are structures containing the state for all servers
type stateImpl struct {
	// TODO maybe we could aggregate all the server map into a map[ServerID]struct{...}
	id                          ServerID
	electionTimeoutStarted      bool
	electionTimer               *time.Timer
	currentElectionVotes        int
	lastSentLogIndex            map[ServerID]int
	currentLeader               ServerID
	currentTerm                 int
	currentIgnoreTerm           int
	acceptAppendEntries         bool
	votedFor                    ServerID
	logs                        [logArrayCapacity]RaftLog
	logHashes                   [logArrayCapacity][32]byte
	nextLogArrayIdx             int
	lastSnapshot                *snapshot
	currentState                instanceState
	commitIndex                 int
	lastApplied                 int
	nextIndex                   map[ServerID]int
	matchIndex                  map[ServerID]int
	oldServerConfiguration      map[ServerID]bool
	serverConfiguration         map[ServerID]bool
	clientState                 map[ServerID]clientStateStruct
	newServerResponseChan       map[ServerID]chan bool
	lock                        *sync.Mutex
	snapshotRequestChan         chan bool
	snapshotResponseChan        chan []byte
	installSnapshotChan         chan []byte
	configurationQueue          []configurationAction
	inConfigurationChange       bool
	pendingConfigurationChanges int
	unvotingServers             map[ServerID]configurationAction
	lastConfigurationLog        int
	nodeKeys                    map[ServerID]*rsa.PublicKey
	clientKeys                  map[string]*rsa.PublicKey
	privateKey                  *rsa.PrivateKey
	bannedServers               []ServerID
	currentVotes                map[ServerID]RequestVoteResponse
	appendEntriesSituation      map[ServerID]appendEntriesSituationStruct
	votedForTerm                int
	votedForChan                chan *RequestVoteResponse
	clientChanResponse          map[string]map[int64]chan bool
}

func raftLogToString(log RaftLog) string {
	if log.Type == Game {
		return fmt.Sprint(log.Log.Id, " ", log.Log.ActionId, " ", log.Log.Type, " ", log.Log.Action)
	}
	return fmt.Sprint(log.ConfigurationLog.Add, " ", log.ConfigurationLog.Server)
}

func newGameRaftLog(idx int, term int, log GameLog) RaftLog {
	var emptyCfgLog = ConfigurationLog{false, "", nil}
	return RaftLog{idx, term, Game, log, emptyCfgLog}
}

func newConfigurationRaftLog(idx int, term int, cfgLog ConfigurationLog) RaftLog {
	var emptyLog = GameLog{"", -1, "", []byte{}, nil}
	return RaftLog{idx, term, Configuration, emptyLog, cfgLog}
}

func newNoopRaftLog(idx int, term int) RaftLog {
	var emptyCfgLog = ConfigurationLog{false, "", nil}
	var emptyLog = GameLog{"", -1, "", []byte{}, nil}
	return RaftLog{idx, term, Noop, emptyLog, emptyCfgLog}
}

// NewState returns an empty state, only used once at the beginning
func newState(id string, otherServers []ServerID, snapshotRequestChan chan bool, snapshotResponseChan chan []byte, installSnapshotChan chan []byte, nodeKeys map[ServerID]*rsa.PublicKey, clientKeys map[string]*rsa.PublicKey, privateKey *rsa.PrivateKey) *stateImpl {
	var lastSentLogIndex = make(map[ServerID]int)
	var nextIndex = make(map[ServerID]int)
	var matchIndex = make(map[ServerID]int)
	var clientState = make(map[ServerID]clientStateStruct)
	var newServerResponseChan = make(map[ServerID]chan bool)
	var lock = &sync.Mutex{}
	var oldServerConfiguration = map[ServerID]bool{ServerID(id): true}
	var serverConfiguration = map[ServerID]bool{ServerID(id): true}
	for _, val := range otherServers {
		oldServerConfiguration[val] = true
		serverConfiguration[val] = true
	}

	var state = stateImpl{
		ServerID(id),
		false,
		nil,
		0,
		lastSentLogIndex,
		"",
		0,
		-1,
		false,
		"",
		[logArrayCapacity]RaftLog{},
		[logArrayCapacity][32]byte{},
		0,
		nil,
		Follower,
		-1,
		-1,
		nextIndex,
		matchIndex,
		oldServerConfiguration,
		serverConfiguration,
		clientState,
		newServerResponseChan,
		lock,
		snapshotRequestChan,
		snapshotResponseChan,
		installSnapshotChan,
		[]configurationAction{},
		false,
		0,
		make(map[ServerID]configurationAction),
		0,
		nodeKeys,
		clientKeys,
		privateKey,
		[]ServerID{},
		make(map[ServerID]RequestVoteResponse),
		make(map[ServerID]appendEntriesSituationStruct),
		-1,
		nil,
		make(map[string]map[int64]chan bool)}
	return &state
}

// State interface for raft server instances
type state interface {
	startElection()
	prepareRequestVoteRPC() *RequestVoteArgs
	getState() instanceState
	checkElectionTimeout() *time.Timer
	stopElectionTimeout()
	handleRequestToVote(requestVoteArgsWrapper) *RequestVoteResponse
	getElectionTimer() *time.Timer
	updateElection(*RequestVoteResponse) bool
	winElection()
	getAppendEntriesArgs(ServerID) *AppendEntriesArgs
	handleAppendEntriesResponse(*AppendEntriesResponse) (int, bool)
	addNewGameLog(GameLog) bool
	addNewConfigurationLog(ConfigurationLog) bool
	handleAppendEntries(*AppendEntriesArgs) (*AppendEntriesResponse, bool)
	updateLastApplied() int
	getLog(int) RaftLog
	checkCommits()
	getCurrentLeader() ServerID
	getID() ServerID
	getCommitIndex() int
	addNewServer(ServerID)
	removeServer(ServerID)
	updateNewServerResponseChans(ServerID, chan bool)
	removeNewServerResponseChan(ServerID)
	getNewServerResponseChan(ServerID) chan bool
	prepareInstallSnapshotRPC() *InstallSnapshotArgs
	handleInstallSnapshotResponse(*InstallSnapshotResponse) int
	handleInstallSnapshotRequest(*InstallSnapshotArgs) *InstallSnapshotResponse
	updateClientLastActionApplied(ServerID, int64)
	getClientLastActionApplied(ServerID) int64
	isBanned(ServerID) bool
	updateAppendEntriesResponseSituation(*AppendEntriesResponse)
	updateCommitIndex()
	ignoreHeartbeatsForThisTerm()
	shouldIgnoreHeartbeats() bool
	hasVoted() bool
	sendPendingVotes()
	clearPendingVotes()
	handleConfigurationRPC(configurationAction) (bool, configurationAction)
	handleNextConfigurationChange() (bool, configurationAction)
	addNewUnvotingServer(configurationAction)
	removeUnvotingServerAction(ServerID) configurationAction
	countConnections() int
	unlockNextConfiguration()
	serverInConfiguration(ServerID) bool
	addClientChanResponse(string, int64, chan bool)
	respondToClientChanResponse(string, int64, bool)
}

func (_state *stateImpl) isBanned(sid ServerID) bool {
	for _, id := range _state.bannedServers {
		if id == sid {
			return true
		}
	}
	return false
}

func (_state *stateImpl) banServer(sid ServerID) {
	log.Info("Ban server: ", sid)
	_state.bannedServers = append(_state.bannedServers, sid)
}

/* To start a new election a server:
 * 1. Increment its current term
 * 2. Changes its current state to Candidate
 * 3. Votes for itself
 */
func (_state *stateImpl) startElection() {
	_state.currentTerm++
	log.Info("Become Candidate: ", _state.currentTerm)
	_state.currentState = Candidate
	_state.votedFor = _state.id
	_state.currentElectionVotes = 1
	_state.currentVotes = make(map[ServerID]RequestVoteResponse)
}

func (_state *stateImpl) prepareRequestVoteRPC() *RequestVoteArgs {
	var lastLogIdx, lastLogTerm = _state.getLastLogIdxTerm()

	var rva = RequestVoteArgs{_state.currentTerm, _state.id, lastLogIdx, lastLogTerm, []byte{}}
	rva.Signature = getRequestVoteArgsSignature(_state.privateKey, &rva)
	return &rva
}

func (_state *stateImpl) getState() instanceState {
	return _state.currentState
}

// Only start a new election timeout if its not already running
func (_state *stateImpl) checkElectionTimeout() *time.Timer {
	if !_state.electionTimeoutStarted {
		// The election timeout is randomized to prevent split votes
		var electionTimeout = time.Duration(time.Millisecond * time.Duration(rand.Intn(maxElectionTimeout-minElectionTimeout)+minElectionTimeout))
		_state.electionTimeoutStarted = true
		_state.electionTimer = time.NewTimer(electionTimeout)
	}
	return _state.electionTimer
}

func (_state *stateImpl) stopElectionTimeout() {
	// Ensure the election timer is actually stopped and the channel empty
	if _state.electionTimer != nil {
		if !_state.electionTimer.Stop() {
			select {
			case <-_state.electionTimer.C:
			default:
			}
		}
		_state.electionTimeoutStarted = false
	}
}

func (_state *stateImpl) updateCurrentTerm(newTerm int) {
	_state.currentState = Follower
	_state.currentTerm = newTerm
	_state.votedFor = ""
}

func (_state *stateImpl) handleRequestToVote(wrap requestVoteArgsWrapper) *RequestVoteResponse {
	_state.lock.Lock()
	var rva = wrap.args
	var hashed = getRequestVoteArgsBytes(rva)
	err := rsa.VerifyPKCS1v15((_state.nodeKeys[(*rva).CandidateID]), crypto.SHA256, hashed[:], (*rva).Signature)
	if err != nil {
		var rvr = RequestVoteResponse{_state.id, _state.currentTerm, false, []byte{}}
		rvr.Signature = getRequestVoteResponseSignature(_state.privateKey, &rvr)
		_state.lock.Unlock()
		log.Info("Refuse vote: unverified signature")
		return &rvr
	}
	var lastLogIdx, lastLogTerm = _state.getLastLogIdxTerm()
	if _state.currentTerm >= (*rva).Term || (*rva).Term > _state.currentTerm+4 {
		log.Info("Current term greater than Candidate term: ", _state.currentTerm, " ", (*rva).Term)
		var rvr = RequestVoteResponse{_state.id, _state.currentTerm, false, []byte{}}
		rvr.Signature = getRequestVoteResponseSignature(_state.privateKey, &rvr)
		_state.lock.Unlock()
		return &rvr
	}

	// At this point we know that (*rva).Term == _state.currentTerm
	if (_state.votedFor == "" || _state.votedFor == (*rva).CandidateID) && (*rva).LastLogTerm >= lastLogTerm && (*rva).LastLogIndex >= lastLogIdx {
		if _state.currentState == Leader {
			_state.currentState = Follower
		}
		_state.votedFor = (*rva).CandidateID
		_state.votedForTerm = (*rva).Term
		_state.votedForChan = nil
		// If there is no current leader vote immediately
		if _state.currentLeader == "" {
			_state.updateCurrentTerm(_state.votedForTerm)
			_state.acceptAppendEntries = false
			log.Info("Immediately send vote to: ", (*rva).CandidateID, " for term ", _state.currentTerm)
			var rvr = RequestVoteResponse{_state.id, _state.currentTerm, true, []byte{}}
			rvr.Signature = getRequestVoteResponseSignature(_state.privateKey, &rvr)
			_state.lock.Unlock()
			return &rvr
		} else {
			log.Info("Cast lazy vote for: ", (*rva).CandidateID)
			_state.votedForChan = wrap.respChan
			_state.lock.Unlock()
			return nil
		}
	}

	log.Info("Already voted for: ", _state.votedFor, " or ", lastLogTerm, " > ", (*rva).LastLogTerm, " or ", lastLogIdx, " > ", (*rva).LastLogIndex)
	var rvr = RequestVoteResponse{_state.id, _state.currentTerm, false, []byte{}}
	rvr.Signature = getRequestVoteResponseSignature(_state.privateKey, &rvr)
	_state.lock.Unlock()
	return &rvr
}

func (_state *stateImpl) getElectionTimer() *time.Timer {
	return _state.electionTimer
}

func (_state *stateImpl) getQuorum() int {
	return int(math.Ceil(float64(2 * len(_state.serverConfiguration) + 1) / 3.0))
}

func (_state *stateImpl) updateElection(resp *RequestVoteResponse) bool {
	_state.lock.Lock()
	var hashed = getRequestVoteResponseBytes(resp)
	err := rsa.VerifyPKCS1v15((_state.nodeKeys[(*resp).Id]), crypto.SHA256, hashed[:], (*resp).Signature)
	if err != nil {
		_state.lock.Unlock()
		return false
	}
	log.Debug("Received RequestVoteRPC response: ", resp.Id, " ", resp.Term, " ", resp.VoteGranted)
	// If the node's current state is stale immediately revert to Follower state
	if (*resp).Term == (_state.currentTerm) && (*resp).VoteGranted == true {
		_state.currentVotes[(*resp).Id] = *resp
		_state.currentElectionVotes++

		log.Debug("Election votes: ", _state.currentElectionVotes, "/", _state.getQuorum())
		if _state.currentElectionVotes >= _state.getQuorum() {
			_state.lock.Unlock()
			_state.winElection()
			return true
		}
	}
	_state.lock.Unlock()
	return false
}

func (_state *stateImpl) winElection() {
	_state.lock.Lock()
	log.Info("Become Leader: ", _state.currentTerm)
	var lastLogIdx, _ = _state.getLastLogIdxTerm()
	_state.currentElectionVotes = 0
	_state.currentState = Leader
	_state.currentLeader = _state.id
	for id := range _state.nextIndex {
		_state.nextIndex[id] = lastLogIdx + 1
	}
	_state.matchIndex[_state.id] = -1
	_state.lastSentLogIndex[_state.id] = -1
	_state.addNewNoopLog()
	_state.lock.Unlock()
}

func (_state *stateImpl) prepareAppendEntriesArgs(lastLogIdx int, lastLogTerm int, lastLogHash [32]byte, logsToSend []RaftLog) *AppendEntriesArgs {
	_state.lock.Lock()
	var aea = AppendEntriesArgs{
		_state.currentTerm,
		_state.id,
		lastLogIdx,
		lastLogTerm,
		lastLogHash,
		logsToSend,
		_state.commitIndex,
		_state.currentVotes,
		[]byte{}}
	aea.Signature = getAppendEntriesArgsSignature(_state.privateKey, &aea)
	_state.lock.Unlock()
	return &aea
}

func (_state *stateImpl) getLastLogIdxTerm() (int, int) {
	if _state.nextLogArrayIdx > 0 {
		var lastLog = _state.logs[_state.nextLogArrayIdx-1]
		return lastLog.Idx, lastLog.Term
	} else if _state.lastSnapshot != nil {
		return _state.lastSnapshot.lastIncludedIndex, _state.lastSnapshot.lastIncludedTerm
	} else {
		return -1, -1
	}
}

func (_state *stateImpl) findArrayIndexByLogIndex(idx int) (bool, int) {
	var found = false
	var result = -1
	for i := _state.nextLogArrayIdx - 1; i >= 0 && !found; i-- {
		if _state.logs[i].Idx == idx {
			found = true
			result = i
		}
	}

	return found, result
}

func (_state *stateImpl) findArrayIndexByHash(hash [32]byte) (bool, int) {
	var found = false
	var result = -1
	for i := _state.nextLogArrayIdx - 1; i >= 0 && !found; i-- {
		if _state.logHashes[i] == hash {
			found = true
			result = i
		}
	}

	return found, result
}

func (_state *stateImpl) getLogHashFromIdx(idx int) (bool, [32]byte) {
	var found, arrIdx = _state.findArrayIndexByLogIndex(idx)
	if !found && _state.lastSnapshot != nil {
		return true, _state.lastSnapshot.hash
	} else if found {
		return true, _state.logHashes[arrIdx]
	}
	return false, [32]byte{}
}

func (_state *stateImpl) getAppendEntriesArgs(id ServerID) *AppendEntriesArgs {
	_state.lock.Lock()
	var myLastLogIdx, myLastLogTerm = _state.getLastLogIdxTerm()
	// Check if the next log for this server is in the log array or in a snapshot
	var serverNextIdx = _state.nextIndex[id]
	var found, arrayLogIdx = _state.findArrayIndexByLogIndex(serverNextIdx)
	if !found && serverNextIdx <= myLastLogIdx {
		// The log is in a snapshot
		_state.lock.Unlock()
		return nil
	}
	var logsToSend = []RaftLog{}
	if arrayLogIdx >= 0 && arrayLogIdx < _state.nextLogArrayIdx {
		logsToSend = _state.logs[arrayLogIdx:_state.nextLogArrayIdx]
	}
	var lastLogIdx = -1
	var lastLogTerm = -1
	var lastLogHash = [32]byte{}
	if arrayLogIdx < 0 {
		lastLogIdx = myLastLogIdx
		lastLogTerm = myLastLogTerm
	} else if arrayLogIdx == 0 && _state.lastSnapshot != nil {
		lastLogIdx = (*_state.lastSnapshot).lastIncludedIndex
		lastLogTerm = (*_state.lastSnapshot).lastIncludedTerm
	} else if arrayLogIdx > 0 {
		var lastLog = _state.logs[arrayLogIdx-1]
		lastLogIdx = lastLog.Idx
		lastLogTerm = lastLog.Term
	}
	// Keep track of the last log actually sent to a follower
	if len(logsToSend) > 0 {
		_state.lastSentLogIndex[id] = logsToSend[len(logsToSend)-1].Idx
		log.Trace("Sending ", len(logsToSend), " logs to node ", id, " (start: ", logsToSend[0].Idx, ", end: ", logsToSend[len(logsToSend)-1].Idx, ")")
	} else {
		_state.lastSentLogIndex[id] = serverNextIdx - 1
	}
	_, lastLogHash = _state.getLogHashFromIdx(lastLogIdx)
	_state.lock.Unlock()
	return _state.prepareAppendEntriesArgs(lastLogIdx, lastLogTerm, lastLogHash, logsToSend)
}

func (_state *stateImpl) getLogHash(raftLog RaftLog) [32]byte {
	var logBytes = getRaftLogBytes(raftLog)
	var _, logArrIdx = _state.findArrayIndexByLogIndex(raftLog.Idx)
	if logArrIdx > 0 {
		log.Trace("Compute hash based on previous log: ", fmt.Sprintf("%x", _state.logHashes[logArrIdx-1]))
		return sha256.Sum256(append(logBytes[:], _state.logHashes[logArrIdx-1][:]...))
	} else if _state.lastSnapshot != nil {
		log.Trace("Compute hash based on snapshot: ", fmt.Sprintf("%x", _state.lastSnapshot.hash))
		return sha256.Sum256(append(logBytes[:], _state.lastSnapshot.hash[:]...))
	}
	log.Trace("Compute hash bsed on raft log only")
	return sha256.Sum256(logBytes[:])
}

func (_state *stateImpl) addNewGameLog(msg GameLog) bool {
	_state.lock.Lock()
	var lastLogIdx, _ = _state.getLastLogIdxTerm()
	var newLog = newGameRaftLog(lastLogIdx+1, _state.currentTerm, msg)
	if _state.nextLogArrayIdx >= logArrayCapacity {
		_state.takeSnapshot()
	}
	if _state.nextLogArrayIdx >= logArrayCapacity {
		_state.lock.Unlock()
		return false
	}
	_state.logs[_state.nextLogArrayIdx] = newLog
	_state.nextLogArrayIdx++
	_state.logHashes[_state.nextLogArrayIdx-1] = _state.getLogHash(newLog)
	log.Info("State - add raft log: ", newLog.Idx, " ", newLog.Term, " ", fmt.Sprintf("%x", _state.logHashes[_state.nextLogArrayIdx-1]))
	_state.matchIndex[_state.id] = newLog.Idx
	_state.lastSentLogIndex[_state.id] = newLog.Idx
	_state.nextIndex[_state.id] = newLog.Idx + 1

	if newLog.Idx > _state.appendEntriesSituation[_state.id].lastIndex {
		_state.appendEntriesSituation[_state.id] = appendEntriesSituationStruct{newLog.Idx, _state.logHashes[_state.nextLogArrayIdx-1]}
	}
	_state.lock.Unlock()
	return true
}

func (_state *stateImpl) handleNewConfiguration(idx int, conf ConfigurationLog) {
	// Keep last configuration
	_state.oldServerConfiguration = make(map[ServerID]bool)
	for key, val := range _state.serverConfiguration {
		_state.oldServerConfiguration[key] = val
	}

	if conf.Add {
		_state.serverConfiguration[conf.Server] = true
	} else {
		delete(_state.serverConfiguration, conf.Server)
	}

	_state.lastConfigurationLog = idx
}

func (_state *stateImpl) addNewConfigurationLog(conf ConfigurationLog) bool {
	var lastLogIdx, _ = _state.getLastLogIdxTerm()
	var newLog = newConfigurationRaftLog(lastLogIdx+1, _state.currentTerm, conf)

	_state.handleNewConfiguration(lastLogIdx+1, conf)

	if _state.nextLogArrayIdx >= logArrayCapacity {
		_state.takeSnapshot()
	}
	if _state.nextLogArrayIdx >= logArrayCapacity {
		return false
	}
	_state.logs[_state.nextLogArrayIdx] = newLog
	_state.nextLogArrayIdx++
	_state.logHashes[_state.nextLogArrayIdx-1] = _state.getLogHash(newLog)
	log.Info("State - add raft log: ", newLog.Idx, " ", newLog.Term, " ", fmt.Sprintf("%x", _state.logHashes[_state.nextLogArrayIdx-1]))
	_state.matchIndex[_state.id] = newLog.Idx
	_state.lastSentLogIndex[_state.id] = newLog.Idx
	_state.nextIndex[_state.id] = newLog.Idx + 1

	if newLog.Idx > _state.appendEntriesSituation[_state.id].lastIndex {
		_state.appendEntriesSituation[_state.id] = appendEntriesSituationStruct{newLog.Idx, _state.logHashes[_state.nextLogArrayIdx-1]}
	}
	return true
}

func (_state *stateImpl) addNewNoopLog() {
	var lastLogIdx, _ = _state.getLastLogIdxTerm()
	var newLog = newNoopRaftLog(lastLogIdx+1, _state.currentTerm)
	if _state.nextLogArrayIdx >= logArrayCapacity {
		_state.takeSnapshot()
	}
	_state.logs[_state.nextLogArrayIdx] = newLog
	_state.nextLogArrayIdx++
	_state.logHashes[_state.nextLogArrayIdx-1] = _state.getLogHash(newLog)
	log.Info("State - add raft log: ", newLog.Idx, " ", newLog.Term, " ", fmt.Sprintf("%x", _state.logHashes[_state.nextLogArrayIdx-1]))
	_state.matchIndex[_state.id] = newLog.Idx
	_state.lastSentLogIndex[_state.id] = newLog.Idx
	_state.nextIndex[_state.id] = newLog.Idx + 1

	if newLog.Idx > _state.appendEntriesSituation[_state.id].lastIndex {
		_state.appendEntriesSituation[_state.id] = appendEntriesSituationStruct{newLog.Idx, _state.logHashes[_state.nextLogArrayIdx-1]}
	}
}

func (_state *stateImpl) verifyAppendEntriesQuorum(aea *AppendEntriesArgs) bool {
	var votes = 1
	for id, val := range (*aea).CurrentVotes {
		var hashed = getRequestVoteResponseBytes(&val)
		err := rsa.VerifyPKCS1v15((_state.nodeKeys[id]), crypto.SHA256, hashed[:], val.Signature)
		if err != nil {
			return false
		}
		if val.VoteGranted {
			votes++
		}
	}

	log.Info("Verify append entries quorum (", (*aea).LeaderID, "): ", votes, "/", _state.getQuorum())
	return votes >= _state.getQuorum()
}

func (_state *stateImpl) updateQuorum(sid ServerID, lastLogIdx int, lastLogHash [32]byte) {
	if lastLogIdx > _state.appendEntriesSituation[sid].lastIndex {
		_state.appendEntriesSituation[sid] = appendEntriesSituationStruct{lastLogIdx, lastLogHash}
	}
}

func (_state *stateImpl) purgeEntriesFromSnapshottedLogs(aea *AppendEntriesArgs) {
	var lastSnapshotIndex = _state.lastSnapshot.lastIncludedIndex
	var elementsToRemove = 0
	for i := 0; i < len((*aea).Entries); i++ {
		if (*aea).Entries[i].Idx <= lastSnapshotIndex {
			elementsToRemove++
		} else {
			break
		}
	}
	if elementsToRemove > 0 {
		(*aea).PrevLogIndex = (*aea).Entries[elementsToRemove-1].Idx
		(*aea).PrevLogTerm = (*aea).Entries[elementsToRemove-1].Term
	}
	copy((*aea).Entries, (*aea).Entries[elementsToRemove:])
	(*aea).Entries = (*aea).Entries[:len((*aea).Entries)-elementsToRemove]
}

func (_state *stateImpl) handleAppendEntries(aea *AppendEntriesArgs) (*AppendEntriesResponse, bool) {
	_state.lock.Lock()
	var hashed = getAppendEntriesArgsBytes(aea)
	err := rsa.VerifyPKCS1v15((_state.nodeKeys[(*aea).LeaderID]), crypto.SHA256, hashed[:], (*aea).Signature)
	if err != nil {
		var aer = AppendEntriesResponse{_state.id, -1, false, -1, [32]byte{}, []byte{}}
		aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
		_state.lock.Unlock()
		log.Debug("Refuse AERPC signature not verified")
		return &aer, false
	}
	var lastLogIdx, _ = _state.getLastLogIdxTerm()
	// 1. Reply false if rpc term < currentTerm
	if (*aea).Term < _state.currentTerm {
		var aer = AppendEntriesResponse{_state.id, _state.currentTerm, false, lastLogIdx, [32]byte{}, []byte{}}
		aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
		_state.lock.Unlock()
		log.Debug("Refuse AERPC term out of date: ", (*aea).Term, " < ", _state.currentTerm)
		return &aer, true
	}

	// At this point a Leader or Candidate should step down because the term is at least
	// as up to date as their own (for Leaders this will be strictly greater)
	if _state.currentState == Candidate {
		// If AppendEntries RPC received from new leader: convert to follower
		_state.stopElectionTimeout()
		log.Info("Become Follower (handleAppendEntries)")
		_state.currentState = Follower
	}

	if !_state.acceptAppendEntries && (*aea).Term > 1 {
		var quorum = _state.verifyAppendEntriesQuorum(aea)
		log.Info("AppendEntries quorum verified: ", quorum)
		if quorum {
			// Immediately update term if out of date
			if (*aea).Term > _state.currentTerm {
				_state.updateCurrentTerm((*aea).Term)
			}
			_state.acceptAppendEntries = true
		} else {
			var aer = AppendEntriesResponse{_state.id, _state.currentTerm, false, lastLogIdx, [32]byte{}, []byte{}}
			aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
			_state.lock.Unlock()
			log.Debug("Refuse AERPC no quorum")
			return &aer, true
		}
	}

	var prevLogIdx = -1
	var found = false
	if (*aea).PrevLogIndex >= 0 {
		// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		found, prevLogIdx = _state.findArrayIndexByLogIndex((*aea).PrevLogIndex)
		if !found {
			// If we didn't find the previous log index it may be included in the last snapshot
			// so we check if there exists a last snapshot and if it includes the previous log index
			if _state.lastSnapshot == nil || (*_state.lastSnapshot).lastIncludedIndex < (*aea).PrevLogIndex {
				var aer = AppendEntriesResponse{_state.id, _state.currentTerm, false, lastLogIdx, [32]byte{}, []byte{}}
				aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
				_state.lock.Unlock()
				log.Debug("Refuse AERPC log index not in snapshot: ", (*aea).PrevLogIndex, " ", lastLogIdx)
				return &aer, true
			}
		} else {
			var prevLogHash = _state.logHashes[prevLogIdx]
			if prevLogHash != (*aea).PrevLogHash {
				var aer = AppendEntriesResponse{_state.id, _state.currentTerm, false, lastLogIdx, [32]byte{}, []byte{}}
				aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
				_state.lock.Unlock()
				log.Debug("Refuse AERPC umatching hashes")
				return &aer, true
			}
		}
	}

	/*
		var prevLogIdx = -1
		var found = false
		if (*aea).PrevLogHash != [32]byte{} {
			// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
			found, prevLogIdx = _state.findArrayIndexByHash((*aea).PrevLogHash)
			if !found {
				// If we didn't find the previous log index it may be included in the last snapshot
				// so we check if there exists a last snapshot and if it includes the previous log index
				if _state.lastSnapshot == nil || (*_state.lastSnapshot).lastIncludedIndex < (*aea).PrevLogIndex {
					var aer = AppendEntriesResponse{_state.id, _state.currentTerm, false, lastLogIdx, [32]byte{}, []byte{}}
					aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
					_state.lock.Unlock()
					log.Trace("Refuse AERPC log index not in snapshot")
					return &aer, true
				}
			}
		}
	*/

	// At this point we can say that if the append entries request is empty
	// then it is an heartbeat an so we can keep _state.currentLeader updated
	_state.currentLeader = (*aea).LeaderID

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	var startNextIdx = prevLogIdx + 1

	// Check if the node has enough log space for all the logs of the AppendEntriesRPC
	if startNextIdx+len((*aea).Entries) >= logArrayCapacity {
		if !_state.takeSnapshot() {
			var aer = AppendEntriesResponse{_state.id, _state.currentTerm, false, lastLogIdx, [32]byte{}, []byte{}}
			aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
			_state.lock.Unlock()
			log.Debug("Refuse AERPC could not take snapshot")
			return &aer, true
		}
		_state.purgeEntriesFromSnapshottedLogs(aea)
		_, prevLogIdx = _state.findArrayIndexByHash((*aea).PrevLogHash)
		startNextIdx = prevLogIdx + 1
		if startNextIdx+len((*aea).Entries) >= logArrayCapacity {
			var aer = AppendEntriesResponse{_state.id, _state.currentTerm, false, lastLogIdx, [32]byte{}, []byte{}}
			aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
			_state.lock.Unlock()
			log.Debug("Refuse AERPC no space after snapshot")
			return &aer, true
		}
	}

	if len((*aea).Entries) > 0 {
		_state.currentIgnoreTerm = -1
	}

	// At this point we should be confident that we have enough space for the logs
	for i := 0; i < len((*aea).Entries); i++ {
		var nextIdx = startNextIdx + i
		if nextIdx >= _state.nextLogArrayIdx {
			_state.logs[nextIdx] = (*aea).Entries[i]
			_state.nextLogArrayIdx = nextIdx + 1
			_state.logHashes[nextIdx] = _state.getLogHash(_state.logs[nextIdx])
			log.Info("State - add raft log: ", _state.logs[nextIdx].Idx, " ", _state.logs[nextIdx].Term, " ", fmt.Sprintf("%x", _state.logHashes[nextIdx]))
			if _state.logs[nextIdx].Type == Configuration {
				_state.handleNewConfiguration(nextIdx, _state.logs[nextIdx].ConfigurationLog)
			}
		} else {
			// If the terms conflict (different index or same index and different terms) remove all the remaining logs
			if _state.logs[nextIdx].Idx != (*aea).Entries[i].Idx || _state.logs[nextIdx].Term != (*aea).Entries[i].Term {
				log.Info("State - Removing logs: ", _state.logs[nextIdx].Idx, " ", _state.logs[_state.nextLogArrayIdx-1].Idx, " (AppendEntriesRPC)")
				// If we are removing a configuration log we need to revert to the previous configuration
				if _state.logs[nextIdx].Idx <= _state.lastConfigurationLog {
					_state.serverConfiguration = make(map[ServerID]bool)
					for key, val := range _state.oldServerConfiguration {
						_state.serverConfiguration[key] = val
					}
				}
				_state.logs[nextIdx] = (*aea).Entries[i]
				_state.nextLogArrayIdx = nextIdx + 1
				_state.logHashes[nextIdx] = _state.getLogHash(_state.logs[nextIdx])
				log.Info("State - add raft log: ", _state.logs[nextIdx].Idx, " ", _state.logs[nextIdx].Term, " ", fmt.Sprintf("%x", _state.logHashes[nextIdx]))
				if _state.logs[nextIdx].Type == Configuration {
					_state.handleNewConfiguration(nextIdx, _state.logs[nextIdx].ConfigurationLog)
				}
			}
			// Otherwise we should be confident that the logs are the same and need not be replaced
		}
	}

	var lastAddedLogIdx, _ = _state.getLastLogIdxTerm()
	_, lastLogHash := _state.getLogHashFromIdx(lastAddedLogIdx)
	if len((*aea).Entries) > 0 {
		_state.updateQuorum(_state.id, lastAddedLogIdx, lastLogHash)
		_state.updateQuorum((*aea).LeaderID, lastAddedLogIdx, lastLogHash)
	}

	var aer = AppendEntriesResponse{_state.id, _state.currentTerm, true, lastAddedLogIdx, lastLogHash, []byte{}}
	aer.Signature = getAppendEntriesResponseSignature(_state.privateKey, &aer)
	_state.lock.Unlock()
	return &aer, true
}

func (_state *stateImpl) handleAppendEntriesResponse(aer *AppendEntriesResponse) (int, bool) {
	_state.lock.Lock()
	var hashed = getAppendEntriesResponseBytes(aer)
	err := rsa.VerifyPKCS1v15((_state.nodeKeys[(*aer).Id]), crypto.SHA256, hashed[:], (*aer).Signature)
	if err != nil {
		_state.lock.Unlock()
		return -1, false
	}

	if (*aer).Term > _state.currentTerm {
		_state.currentTerm = (*aer).Term
		log.Debug("Leader: Update current term: ", _state.currentTerm)
	}

	if !(*aer).Success {
		log.Debug("Unsuccessful AppendEntriesRPC: ", (*aer).Id)
		var snapshot = false
		if (*aer).LastIndex >= 0 {
			_state.nextIndex[(*aer).Id] = (*aer).LastIndex
			log.Debug("Fix next index for : ", (*aer).Id, " ", _state.nextIndex[(*aer).Id])
		} else {
			_state.nextIndex[(*aer).Id] = 0
		}
		if _state.lastSnapshot != nil && _state.nextIndex[(*aer).Id] <= (*_state.lastSnapshot).lastIncludedIndex {
			snapshot = true
		}
		_state.lock.Unlock()
		return -1, snapshot
	}
	_state.nextIndex[(*aer).Id] = _state.lastSentLogIndex[(*aer).Id] + 1
	_state.matchIndex[(*aer).Id] = _state.lastSentLogIndex[(*aer).Id]
	if _state.lastSentLogIndex[(*aer).Id] > _state.appendEntriesSituation[(*aer).Id].lastIndex {
		_, lastSentLogHash := _state.getLogHashFromIdx(_state.lastSentLogIndex[(*aer).Id])
		_state.appendEntriesSituation[(*aer).Id] = appendEntriesSituationStruct{_state.lastSentLogIndex[(*aer).Id], lastSentLogHash}
	}
	var matchIndex = _state.matchIndex[(*aer).Id]
	_state.lock.Unlock()
	return matchIndex, false
}

func (_state *stateImpl) updateLastApplied() int {
	if _state.commitIndex > _state.lastApplied {
		var _, logIdx = _state.findArrayIndexByLogIndex(_state.lastApplied + 1)
		if logIdx >= 0 {
			_state.lastApplied++
		}
		log.Info("State: update last applied (log: ", _state.lastApplied, ", arr: ", logIdx, ", commit: ", _state.commitIndex, ")")
		return logIdx
	}
	return -1
}

func (_state *stateImpl) getLog(i int) RaftLog {
	//var _, logIdx = _state.findArrayIndexByLogIndex(i)
	return _state.logs[i]
}

func (_state *stateImpl) checkCommits() {
	_state.lock.Lock()
	if len(_state.logs) < 1 {
		return
	}
	var bound = false
	for i := _state.nextLogArrayIdx - 1; !bound && i >= 0; i-- {
		// i > commitIndex
		if _state.logs[i].Idx <= _state.commitIndex {
			bound = true
			continue
		}
		// A majority of matchIndex[j] >= i
		var replicatedFollowers = 0
		for id := range _state.matchIndex {
			if _state.matchIndex[id] >= _state.logs[i].Idx {
				replicatedFollowers++
			}
		}
		if replicatedFollowers >= _state.getQuorum() {
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
	if _, found := _state.nextIndex[sid]; !found {
		_state.lastSentLogIndex[sid] = 0
		_state.nextIndex[sid] = 0
		_state.matchIndex[sid] = 0
		_state.appendEntriesSituation[sid] = appendEntriesSituationStruct{-1, [32]byte{}}
	}
	_state.lock.Unlock()
}

func (_state *stateImpl) removeServer(sid ServerID) {
	_state.lock.Lock()
	delete(_state.clientState, sid)
	delete(_state.serverConfiguration, sid)
	delete(_state.appendEntriesSituation, sid)
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

func (_state *stateImpl) copyLogsToBeginningOfRecord(startLog int) {
	var restartIdx = 0
	for startLog+restartIdx < _state.nextLogArrayIdx {
		log.Trace("Copy log from ", startLog+restartIdx, "(", _state.logs[startLog+restartIdx].Idx, ")", "to ", restartIdx)
		_state.logs[restartIdx] = _state.logs[startLog+restartIdx]
		_state.logHashes[restartIdx] = _state.logHashes[startLog+restartIdx]
		restartIdx++
	}
	_state.nextLogArrayIdx = restartIdx
}

func (_state *stateImpl) takeSnapshot() bool {
	_state.snapshotRequestChan <- true
	currentGameState := <-_state.snapshotResponseChan
	var found, lastAppliedIdx = _state.findArrayIndexByLogIndex(_state.lastApplied)
	if !found {
		return false
	}
	log.Trace("Taking snapshot (arr: ", lastAppliedIdx, ", idx: ", _state.logs[lastAppliedIdx].Idx, ", term: ", _state.logs[lastAppliedIdx].Term, ")")
	var newSnapshot = snapshot{
		_state.serverConfiguration,
		currentGameState,
		_state.logs[lastAppliedIdx].Idx,
		_state.logs[lastAppliedIdx].Term,
		_state.logHashes[lastAppliedIdx]}
	_state.lastSnapshot = &newSnapshot
	// Copy remaining logs at the start of the log array
	_state.copyLogsToBeginningOfRecord(lastAppliedIdx + 1)
	return true
}

func (_state *stateImpl) prepareInstallSnapshotRPC() *InstallSnapshotArgs {
	var lastSnapshot = *_state.lastSnapshot
	var newInstallSnapshotArgs = InstallSnapshotArgs{
		_state.id,
		_state.currentTerm,
		lastSnapshot.lastIncludedIndex,
		lastSnapshot.lastIncludedTerm,
		lastSnapshot.gameState,
		lastSnapshot.serverConfiguration,
		lastSnapshot.hash,
		[]byte{}}
	newInstallSnapshotArgs.Signature = getInstallSnapshotArgsSignature(_state.privateKey, &newInstallSnapshotArgs)
	return &newInstallSnapshotArgs
}

func (_state *stateImpl) handleInstallSnapshotResponse(isr *InstallSnapshotResponse) int {
	_state.lock.Lock()
	var hashed = getInstallSnapshotResponseBytes(isr)
	err := rsa.VerifyPKCS1v15((_state.nodeKeys[(*isr).Id]), crypto.SHA256, hashed[:], (*isr).Signature)
	if err != nil {
		_state.lock.Unlock()
		return -1
	}
	if !(*isr).Success {
		_state.lock.Unlock()
		return -1
	}
	_state.lastSentLogIndex[(*isr).Id] = (*isr).LastIncludedIndex
	_state.nextIndex[(*isr).Id] = (*isr).LastIncludedIndex + 1

	_state.matchIndex[(*isr).Id] = (*isr).LastIncludedIndex
	var matchIndex = _state.matchIndex[(*isr).Id]
	_state.lock.Unlock()
	return matchIndex
}

func (_state *stateImpl) handleInstallSnapshotRequest(isa *InstallSnapshotArgs) *InstallSnapshotResponse {
	_state.lock.Lock()
	log.Trace("Received install snapshot request")
	var hashed = getInstallSnapshotArgsBytes(isa)
	err := rsa.VerifyPKCS1v15((_state.nodeKeys[(*isa).Id]), crypto.SHA256, hashed[:], (*isa).Signature)
	if err != nil {
		var isr = InstallSnapshotResponse{_state.id, _state.currentTerm, false, -1, -1, []byte{}}
		isr.Signature = getInstallSnapshotResponseSignature(_state.privateKey, &isr)
		return &isr
	}
	if (*isa).Term < _state.currentTerm {
		var isr = InstallSnapshotResponse{_state.id, _state.currentTerm, false, -1, -1, []byte{}}
		isr.Signature = getInstallSnapshotResponseSignature(_state.privateKey, &isr)
		_state.lock.Unlock()
		return &isr
	}

	if (*isa).LastIncludedIndex <= _state.lastApplied {
		var isr = InstallSnapshotResponse{_state.id, _state.currentTerm, true, (*isa).LastIncludedIndex, (*isa).LastIncludedTerm, []byte{}}
		isr.Signature = getInstallSnapshotResponseSignature(_state.privateKey, &isr)
		_state.lock.Unlock()
		return &isr
	}

	var lastLogIdx, _ = _state.getLastLogIdxTerm()
	if (*isa).LastIncludedIndex < lastLogIdx {
		var found, lastIncludedArrIdx = _state.findArrayIndexByLogIndex((*isa).LastIncludedIndex)
		if found {
			log.Info("State - Removing logs: ", _state.logs[lastIncludedArrIdx].Idx, " ", _state.logs[_state.nextLogArrayIdx-1].Idx, " (InstallSnapshotRPC)")
		}
	}

	// Apply snapshot
	var newSnapshot = snapshot{
		(*isa).ServerConfiguration,
		(*isa).Data,
		(*isa).LastIncludedIndex,
		(*isa).LastIncludedTerm,
		(*isa).Hash}
	_state.lastSnapshot = &newSnapshot

	_state.nextLogArrayIdx = 0
	_state.commitIndex = (*isa).LastIncludedIndex
	_state.lastApplied = (*isa).LastIncludedIndex
	_state.installSnapshotChan <- (*isa).Data

	log.Trace("State: install snapshot ", (*isa).LastIncludedIndex)
	var isr = InstallSnapshotResponse{_state.id, _state.currentTerm, true, (*isa).LastIncludedIndex, (*isa).LastIncludedTerm, []byte{}}
	isr.Signature = getInstallSnapshotResponseSignature(_state.privateKey, &isr)
	_state.lock.Unlock()
	return &isr
}

func (_state *stateImpl) getClientLastActionApplied(sid ServerID) int64 {
	_state.lock.Lock()
	var result = _state.clientState[sid].lastActionApplied
	_state.lock.Unlock()
	return result
}

func (_state *stateImpl) updateClientLastActionApplied(sid ServerID, idx int64) {
	_state.lock.Lock()
	var previousState = _state.clientState[sid]
	previousState.lastActionApplied = idx
	_state.clientState[sid] = previousState
	_state.lock.Unlock()
}

func (_state *stateImpl) checkLogHash(idx int, hash [32]byte) bool {
	found, myHash := _state.getLogHashFromIdx(idx)
	if found && myHash == hash {
		return true
	}
	return false
}

func (_state *stateImpl) findQuorum() int {
	var votes = map[int]int{}
	for _, val := range _state.appendEntriesSituation {
		votes[val.lastIndex] = 0
	}

	for _, val := range _state.appendEntriesSituation {
		if _state.checkLogHash(val.lastIndex, val.lastHash) {
			for i := range votes {
				if i <= val.lastIndex {
					votes[i]++
				}
			}
		}
	}

	var result = -1
	for idx, val := range votes {
		if val >= _state.getQuorum() && idx > result {
			result = idx
		}
	}

	log.Debug("Find quorum: ", result, " ", votes, " target: ", _state.getQuorum())
	return result
}

func (_state *stateImpl) updateCommitIndex() {
	_state.lock.Lock()
	var quorum = _state.findQuorum()
	if quorum >= 0 && quorum > _state.commitIndex {
		_state.commitIndex = quorum
	}
	_state.lock.Unlock()
}

func (_state *stateImpl) updateAppendEntriesResponseSituation(aer *AppendEntriesResponse) {
	_state.lock.Lock()
	log.Trace("Update append entries response situation ", (*aer).Id, " ", (*aer).LastIndex)
	var hashed = getAppendEntriesResponseBytes(aer)
	err := rsa.VerifyPKCS1v15((_state.nodeKeys[(*aer).Id]), crypto.SHA256, hashed[:], (*aer).Signature)
	if err != nil {
		_state.lock.Unlock()
		return
	}

	if _, found := _state.appendEntriesSituation[(*aer).Id]; !found {
		_state.appendEntriesSituation[(*aer).Id] = appendEntriesSituationStruct{-1, [32]byte{}}
	}

	if (*aer).LastIndex > _state.appendEntriesSituation[(*aer).Id].lastIndex {
		_state.appendEntriesSituation[(*aer).Id] = appendEntriesSituationStruct{(*aer).LastIndex, (*aer).LastHash}
	}

	_state.lock.Unlock()
	return
}

func (_state *stateImpl) ignoreHeartbeatsForThisTerm() {
	_state.lock.Lock()
	_state.currentIgnoreTerm = _state.currentTerm
	_state.lock.Unlock()
}

func (_state *stateImpl) shouldIgnoreHeartbeats() bool {
	_state.lock.Lock()
	var result = _state.currentIgnoreTerm == _state.currentTerm
	_state.lock.Unlock()
	return result
}

func (_state *stateImpl) hasVoted() bool {
	return _state.votedFor != "" && _state.votedForChan != nil
}

func (_state *stateImpl) sendPendingVotes() {
	_state.updateCurrentTerm(_state.votedForTerm)
	_state.acceptAppendEntries = false
	var rvr = RequestVoteResponse{_state.id, _state.currentTerm, true, []byte{}}
	rvr.Signature = getRequestVoteResponseSignature(_state.privateKey, &rvr)
	_state.votedForChan <- &rvr
	log.Info("Actually send lazy vote to: ", _state.votedFor)
	_state.votedForChan = nil
	_state.votedFor = ""
}

func (_state *stateImpl) clearPendingVotes() {
	var rvr = RequestVoteResponse{_state.id, _state.currentTerm, false, []byte{}}
	rvr.Signature = getRequestVoteResponseSignature(_state.privateKey, &rvr)
	_state.votedForChan <- &rvr
	log.Info("Clear lazy vote for: ", _state.votedFor)
	_state.votedForChan = nil
	_state.votedFor = ""
}
func (_state *stateImpl) handleConfigurationRPC(conf configurationAction) (bool, configurationAction) {
	_state.lock.Lock()
	_state.configurationQueue = append(_state.configurationQueue, conf)
	_state.pendingConfigurationChanges++
	log.Trace("Add configuration change to queue: ", _state.pendingConfigurationChanges)
	_state.lock.Unlock()
	return _state.handleNextConfigurationChange()
}

func (_state *stateImpl) handleNextConfigurationChange() (bool, configurationAction) {
	_state.lock.Lock()
	var result configurationAction
	var ok = false
	if !_state.inConfigurationChange && _state.pendingConfigurationChanges > 0 {
		ok = true
		_state.inConfigurationChange = true
		result = _state.configurationQueue[0]
		copy(_state.configurationQueue, _state.configurationQueue[1:])
		_state.configurationQueue = _state.configurationQueue[:len(_state.configurationQueue)-1]
		_state.addNewConfigurationLog(result.Msg)
		_state.pendingConfigurationChanges--
		log.Trace("Remove configuration change from queue: ", _state.pendingConfigurationChanges)
	}
	_state.lock.Unlock()
	return ok, result
}

func (_state *stateImpl) addNewUnvotingServer(conf configurationAction) {
	_state.lock.Lock()
	_state.unvotingServers[conf.Msg.Server] = conf
	_state.lock.Unlock()
}

func (_state *stateImpl) removeUnvotingServerAction(sid ServerID) configurationAction {
	_state.lock.Lock()
	newAction := _state.unvotingServers[sid]
	delete(_state.unvotingServers, sid)
	_state.lock.Unlock()
	return newAction
}

func (_state *stateImpl) countConnections() int {
	_state.lock.Lock()
	var result = len(_state.serverConfiguration)
	_state.lock.Unlock()
	return result
}

func (_state *stateImpl) unlockNextConfiguration() {
	_state.lock.Lock()
	_state.inConfigurationChange = false
	_state.lock.Unlock()
}

func (_state *stateImpl) serverInConfiguration(sid ServerID) bool {
	_state.lock.Lock()
	if _, found := _state.serverConfiguration[sid]; found {
		_state.lock.Unlock()
		return true
	}
	_state.lock.Unlock()
	return false
}

func (_state *stateImpl) addClientChanResponse(clientID string, actionID int64, respChan chan bool) {
	_state.lock.Lock()
	clientMap, found := _state.clientChanResponse[clientID]
	if !found {
		clientMap = make(map[int64]chan bool)
	}

	actionChan, actionFound := _state.clientChanResponse[clientID][actionID]
	if actionFound {
		actionChan <- false
	}
	clientMap[actionID] = respChan
	_state.clientChanResponse[clientID] = clientMap
	_state.lock.Unlock()
}

func (_state *stateImpl) respondToClientChanResponse(clientID string, actionID int64, val bool) {
	_state.lock.Lock()
	clientMap, clientFound := _state.clientChanResponse[clientID]
	if clientFound {
		actionChan, actionFound := clientMap[actionID]
		if actionFound {
			actionChan <- val
			delete(clientMap, actionID)
			_state.clientChanResponse[clientID] = clientMap
		}
	}
	_state.lock.Unlock()
}
