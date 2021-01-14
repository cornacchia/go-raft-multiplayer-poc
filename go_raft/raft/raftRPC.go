package raft

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
	Term        int
	VoteGranted bool
}

type RaftListener struct {
	AppendEntriesArgsChan     chan *AppendEntriesArgs
	AppendEntriesResponseChan chan *AppendEntriesResponse
	RequestVoteArgsChan       chan *RequestVoteArgs
	RequestVoteResponseChan   chan *RequestVoteResponse
}

func initRaftListener(lstOptions *options) *RaftListener {
	return &RaftListener{
		(*lstOptions).appendEntriesArgsChan,
		(*lstOptions).appendEntriesResponseChan,
		(*lstOptions).requestVoteArgsChan,
		(*lstOptions).requestVoteResponseChan}
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
	reply.Term = repl.Term
	reply.VoteGranted = repl.VoteGranted

	return nil
}
