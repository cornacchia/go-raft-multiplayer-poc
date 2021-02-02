package raft

import "fmt"

type AppendEntriesArgs struct {
	Term         int
	LeaderID     ServerID
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Id      ServerID
	Term    int
	Success bool
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
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
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
}

type RaftListener struct {
	AppendEntriesArgsChan      chan *AppendEntriesArgs
	AppendEntriesResponseChan  chan *AppendEntriesResponse
	RequestVoteArgsChan        chan *RequestVoteArgs
	RequestVoteResponseChan    chan *RequestVoteResponse
	InstallSnapshotArgsChan    chan *InstallSnapshotArgs
	InstallSnapshtResponseChan chan *InstallSnapshotResponse
	MessageChan                chan gameAction
}

func initRaftListener(lstOptions *options) *RaftListener {
	return &RaftListener{
		(*lstOptions).appendEntriesArgsChan,
		(*lstOptions).appendEntriesResponseChan,
		(*lstOptions).requestVoteArgsChan,
		(*lstOptions).requestVoteResponseChan,
		(*lstOptions).installSnapshotArgsChan,
		(*lstOptions).installSnapshotResponseChan,
		(*lstOptions).msgChan}
}

func (listener *RaftListener) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesResponse) error {
	listener.AppendEntriesArgsChan <- args
	repl := <-listener.AppendEntriesResponseChan
	reply.Id = repl.Id
	reply.Term = repl.Term
	reply.Success = repl.Success
	// TODO handle timeout
	return nil
}

func (listener *RaftListener) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteResponse) error {
	listener.RequestVoteArgsChan <- args
	repl := <-listener.RequestVoteResponseChan
	reply.Id = repl.Id
	reply.Term = repl.Term
	reply.VoteGranted = repl.VoteGranted

	return nil
}

func (listener *RaftListener) ActionRPC(args *ActionArgs, reply *ActionResponse) error {
	chanApplied := make(chan bool, 1)
	chanResponse := make(chan *ActionResponse)
	var act = gameAction{
		GameLog{fmt.Sprint((*args).Id), (*args).ActionId, (*args).Type, (*args).Action, chanApplied},
		chanResponse}
	listener.MessageChan <- act
	repl := <-chanResponse
	reply.Applied = repl.Applied
	reply.LeaderID = repl.LeaderID
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
	// TODO handle timeout
	return nil
}
