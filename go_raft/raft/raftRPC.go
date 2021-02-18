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
	ServerConfiguration map[ServerID]bool
	Hash                [32]byte
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

type AddRemoveServerArgs struct {
	Server ServerID
	Add    bool
}

type AddRemoveServerResponse struct {
	Success  bool
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
	ConfigurationChan          chan configurationAction
}

func initRaftListener(lstOptions *options) *RaftListener {
	return &RaftListener{
		(*lstOptions).appendEntriesArgsChan,
		(*lstOptions).appendEntriesResponseChan,
		(*lstOptions).requestVoteArgsChan,
		(*lstOptions).requestVoteResponseChan,
		(*lstOptions).installSnapshotArgsChan,
		(*lstOptions).installSnapshotResponseChan,
		(*lstOptions).msgChan,
		(*lstOptions).confChan}
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
	log.Info("Respond to InstallSnapshotRPC: ", (*args).Id, " ", repl.Success)
	return nil
}

func (listener *RaftListener) AddRemoveServerRPC(args *AddRemoveServerArgs, reply *AddRemoveServerResponse) error {
	chanApplied := make(chan bool, 1)
	chanResponse := make(chan *AddRemoveServerResponse)
	var act = configurationAction{
		ConfigurationLog{args.Add, args.Server, chanApplied},
		chanResponse}

	listener.ConfigurationChan <- act
	repl := <-chanResponse
	reply.Success = repl.Success
	reply.LeaderID = repl.LeaderID
	log.Info("Respond to AddRemoveServerRPC: ", (*args).Server, " ", repl.Success)
	return nil
}
