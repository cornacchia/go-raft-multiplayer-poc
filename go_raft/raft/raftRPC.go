package raft

import (
	"crypto"
	"crypto/rsa"
	"fmt"

	log "github.com/sirupsen/logrus"
)

type AppendEntriesArgs struct {
	Term            int
	LeaderID        ServerID
	PrevLogIndex    int
	PrevLogTerm     int
	PrevLogHash     [32]byte
	Entries         []RaftLog
	LeaderCommit    int
	CurrentVotesNew map[ServerID]RequestVoteResponse
	CurrentVotesOld map[ServerID]RequestVoteResponse
	Signature       []byte
}

type AppendEntriesResponse struct {
	Id        ServerID
	Term      int
	Success   bool
	LastIndex int
	Signature []byte
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  ServerID
	LastLogIndex int
	LastLogTerm  int
	Signature    []byte
}

type RequestVoteResponse struct {
	Id          ServerID
	Term        int
	VoteGranted bool
	Signature   []byte
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
	Hash                [32]byte
	Signature           []byte
}

type InstallSnapshotResponse struct {
	Id                ServerID
	Term              int
	Success           bool
	LastIncludedIndex int
	LastIncludedTerm  int
	Signature         []byte
}

type ActionArgs struct {
	Id        string
	ActionId  int64
	Type      string
	Action    []byte
	Signature []byte
}

type ActionResponse struct {
	Applied  bool
	LeaderID ServerID
	// Signature []byte
}

type UpdateLeaderArgs struct {
	Id        string
	LeaderID  ServerID
	Signature []byte
}

type UpdateLeaderResponse struct {
	Success  bool
	LeaderID ServerID
}

type RaftListener struct {
	AppendEntriesArgsChan          chan *AppendEntriesArgs
	AppendEntriesResponseChan      chan *AppendEntriesResponse
	OtherAppendEntriesResponseChan chan *AppendEntriesResponse
	RequestVoteArgsChan            chan *RequestVoteArgs
	RequestVoteResponseChan        chan *RequestVoteResponse
	InstallSnapshotArgsChan        chan *InstallSnapshotArgs
	InstallSnapshtResponseChan     chan *InstallSnapshotResponse
	MessageChan                    chan gameAction
	UpdateLeaderArgsChan           chan *UpdateLeaderArgs
	UpdateLeaderResponseChan       chan *UpdateLeaderResponse
	clientKeys                     map[string]*rsa.PublicKey
}

func initRaftListener(lstOptions *options) *RaftListener {
	return &RaftListener{
		(*lstOptions).appendEntriesArgsChan,
		(*lstOptions).appendEntriesResponseChan,
		(*lstOptions).otherAppendEntriesResponseChan,
		(*lstOptions).requestVoteArgsChan,
		(*lstOptions).requestVoteResponseChan,
		(*lstOptions).installSnapshotArgsChan,
		(*lstOptions).installSnapshotResponseChan,
		(*lstOptions).msgChan,
		(*lstOptions).updateLeaderArgsChan,
		(*lstOptions).updateLeaderResponseChan,
		(*lstOptions).clientKeys}
}

func (listener *RaftListener) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesResponse) error {
	listener.AppendEntriesArgsChan <- args
	repl := <-listener.AppendEntriesResponseChan
	reply.Id = repl.Id
	reply.Term = repl.Term
	reply.Success = repl.Success
	reply.LastIndex = repl.LastIndex
	reply.Signature = repl.Signature
	if len((*args).Entries) > 0 {
		log.Info("Respond to AppendEntriesRPC: ", (*args).LeaderID, " ", (*args).PrevLogIndex)
	}
	// TODO handle timeout
	return nil
}

func (listener *RaftListener) AppendEntriesResponseRPC(args *AppendEntriesResponse, reply *bool) error {
	listener.OtherAppendEntriesResponseChan <- args
	*reply = true
	return nil
}

func (listener *RaftListener) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteResponse) error {
	listener.RequestVoteArgsChan <- args
	repl := <-listener.RequestVoteResponseChan
	reply.Id = repl.Id
	reply.Term = repl.Term
	reply.VoteGranted = repl.VoteGranted
	reply.Signature = repl.Signature
	log.Info("Respond to RequestVoteRPC: ", (*args).CandidateID, " ", repl.VoteGranted)
	return nil
}

func (listener *RaftListener) ActionRPC(args *ActionArgs, reply *ActionResponse) error {
	var hashed = GetActionArgsBytes(args)
	err := rsa.VerifyPKCS1v15((listener.clientKeys[(*args).Id]), crypto.SHA256, hashed[:], (*args).Signature)
	if err != nil {
		reply.Applied = false
		reply.LeaderID = ""
	} else {
		chanApplied := make(chan bool, 1)
		chanResponse := make(chan *ActionResponse)
		var act = gameAction{
			GameLog{fmt.Sprint((*args).Id), (*args).ActionId, (*args).Type, (*args).Action, chanApplied},
			chanResponse}
		listener.MessageChan <- act
		repl := <-chanResponse
		reply.Applied = repl.Applied
		reply.LeaderID = repl.LeaderID
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
	reply.Signature = repl.Signature
	log.Info("Respond to InstallSnapshotRPC: ", (*args).Id, " ", repl.Success)
	// TODO handle timeout
	return nil
}

func (listener *RaftListener) UpdateLeaderRPC(args *UpdateLeaderArgs, reply *UpdateLeaderResponse) error {
	var hashed = GetUpdateLeaderArgsBytes(args)
	err := rsa.VerifyPKCS1v15((listener.clientKeys[(*args).Id]), crypto.SHA256, hashed[:], (*args).Signature)
	if err != nil {
		reply.Success = false
		reply.LeaderID = ""
	} else {
		listener.UpdateLeaderArgsChan <- args
		repl := <-listener.UpdateLeaderResponseChan
		reply.Success = repl.Success
		reply.LeaderID = repl.LeaderID
		log.Info("Respond to UpdateLeaderRPC: ", (*args).LeaderID, " ", repl.Success)
	}
	return nil
}
