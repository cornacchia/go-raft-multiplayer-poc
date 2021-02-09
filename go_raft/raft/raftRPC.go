package raft

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     ServerID
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Id        ServerID
	Term      int
	Success   bool
	LastIndex int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  ServerID
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Id          ServerID
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Id                  ServerID
	Term                int
	LastIncludedIndex   int
	LastIncludedTerm    int
	Data                []byte
	ServerConfiguration map[ServerID][2]bool
	OldServerCount      int
	NewServerCount      int
}

type InstallSnapshotResponse struct {
	Id                ServerID
	Term              int
	Success           bool
	LastIncludedIndex int
	LastIncludedTerm  int
}

type ActionArgs struct {
	Id       string
	ActionId int64
	Type     string
	Action   []byte
}

type ActionResponse struct {
	Applied  bool
	LeaderID ServerID
	State    []byte
}

type RaftListener struct {
	AppendEntriesArgsChan      chan *AppendEntriesArgs
	AppendEntriesResponseChan  chan *AppendEntriesResponse
	RequestVoteArgsChan        chan *RequestVoteArgs
	RequestVoteResponseChan    chan *RequestVoteResponse
	InstallSnapshotArgsChan    chan *InstallSnapshotArgs
	InstallSnapshtResponseChan chan *InstallSnapshotResponse
	MessageChan                chan gameAction
	StateReqChan               chan bool
	StateResChan               chan []byte
}

func initRaftListener(lstOptions *options) *RaftListener {
	stateReqChan := make(chan bool)
	stateResChan := make(chan []byte)
	go handleState((*lstOptions).stateChan, stateReqChan, stateResChan)
	return &RaftListener{
		(*lstOptions).appendEntriesArgsChan,
		(*lstOptions).appendEntriesResponseChan,
		(*lstOptions).requestVoteArgsChan,
		(*lstOptions).requestVoteResponseChan,
		(*lstOptions).installSnapshotArgsChan,
		(*lstOptions).installSnapshotResponseChan,
		(*lstOptions).msgChan,
		stateReqChan,
		stateResChan}
}

func handleState(stateUpdateChan chan []byte, stateReqChan chan bool, stateResChan chan []byte) {
	var state []byte
	for {
		select {
		case newState := <-stateUpdateChan:
			state = newState
		case <-stateReqChan:
			stateResChan <- state
		}
	}
}

func (listener *RaftListener) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesResponse) error {
	listener.AppendEntriesArgsChan <- args
	repl := <-listener.AppendEntriesResponseChan
	reply.Id = repl.Id
	reply.Term = repl.Term
	reply.Success = repl.Success
	reply.LastIndex = repl.LastIndex
	if len((*args).Entries) > 0 {
		log.Info("Respond to AppendEntriesRPC: ", (*args).LeaderID, " ", (*args).PrevLogIndex, " ", reply)
	}
	// TODO handle timeout
	return nil
}

func (listener *RaftListener) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteResponse) error {
	listener.RequestVoteArgsChan <- args
	repl := <-listener.RequestVoteResponseChan
	reply.Id = repl.Id
	reply.Term = repl.Term
	reply.VoteGranted = repl.VoteGranted
	log.Info("Respond to RequestVoteRPC: ", (*args).CandidateID, " ", repl.VoteGranted)
	return nil
}

func (listener *RaftListener) ActionRPC(args *ActionArgs, reply *ActionResponse) error {
	if (*args).Type == "NOOP" {
		listener.StateReqChan <- true
		reply.Applied = true
		reply.LeaderID = ""
		reply.State = <-listener.StateResChan
	} else {
		chanApplied := make(chan bool, 1)
		chanResponse := make(chan *ActionResponse)
		var act = gameAction{
			GameLog{fmt.Sprint((*args).Id), (*args).ActionId, (*args).Type, (*args).Action, chanApplied},
			chanResponse}
		listener.MessageChan <- act
		repl := <-chanResponse
		listener.StateReqChan <- true
		reply.Applied = repl.Applied
		reply.LeaderID = repl.LeaderID
		reply.State = <-listener.StateResChan
	}
	return nil
}

func (listener *RaftListener) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotResponse) error {
	listener.InstallSnapshotArgsChan <- args
	repl := <-listener.InstallSnapshtResponseChan
	reply.Id = repl.Id
	reply.Term = repl.Term
	reply.Success = repl.Success
	reply.LastIncludedIndex = repl.LastIncludedIndex
	reply.LastIncludedTerm = repl.LastIncludedTerm
	log.Info("Respond to InstallSnapshotRPC: ", (*args).Id, " ", repl.Success)
	// TODO handle timeout
	return nil
}
