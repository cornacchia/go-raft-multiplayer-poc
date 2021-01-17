package raft

import (
	"fmt"
	"go_raft/engine"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type raftConnection struct {
	id         ServerID
	connection *rpc.Client
}

type options struct {
	mode   string
	_state state
	// This is used to receive AppendEntriesRPC arguments from other nodes (through the listener)
	appendEntriesArgsChan chan *AppendEntriesArgs
	// This is used to send AppendEntriesRPC responses to the other nodes (through the listener)
	appendEntriesResponseChan chan *AppendEntriesResponse
	// This is used to get responses from remote nodes when sending an AppendEntriesRPC
	myAppendEntriesResponseChan chan *AppendEntriesResponse
	// This is used to receiv RequestVoteRPC arguments from the other nodes (through the listener)
	requestVoteArgsChan chan *RequestVoteArgs
	// This is used to send RequestVoteRPC responses to the other nodes (through the listener)
	requestVoteResponseChan chan *RequestVoteResponse
	// This is used to get responses from remote nodes when sending a RequestVoteRPC
	myRequestVoteResponseChan chan *RequestVoteResponse
	// This is used to receive messages from clients RPC
	msgChan chan engine.GameLog
	// This is used to send responses to actions RPC
	msgResponseChan chan *ActionResponse
	// This is used to send messages to the game engine
	actionChan chan engine.GameLog
	// TODO usare sync.Map https://golang.org/pkg/sync/#Map
	connections         *sync.Map
	numberOfConnections int
}

// Start function for server logic
func Start(mode string, port string, otherServers []ServerID, actionChan chan engine.GameLog) *sync.Map {
	msgChan := make(chan engine.GameLog)
	var newOptions = &options{
		mode,
		newState(port, otherServers),
		make(chan *AppendEntriesArgs),
		make(chan *AppendEntriesResponse),
		make(chan *AppendEntriesResponse),
		make(chan *RequestVoteArgs),
		make(chan *RequestVoteResponse),
		make(chan *RequestVoteResponse),
		msgChan,
		make(chan *ActionResponse),
		actionChan,
		nil,
		0}
	var raftListener = initRaftListener(newOptions)
	startListeningServer(raftListener, port)
	nodeConnections, num := ConnectToRaftServers(newOptions._state.getID(), otherServers)
	newOptions.connections = nodeConnections
	newOptions.numberOfConnections = num
	go run(newOptions)
	return nodeConnections
}

func connectToRaftServer(serverPort ServerID, result chan *raftConnection) {
	var startTime = time.Now().Unix()
	var connected = false
	for time.Now().Unix() < startTime+600 && !connected {
		client, err := rpc.DialHTTP("tcp", "127.0.0.1:"+string(serverPort))
		if err != nil {
			fmt.Println("Error connecting to node: " + string(serverPort))
			time.Sleep(1 * time.Second)
		} else {
			connected = true
			fmt.Println("Connected to node: " + string(serverPort))
			var newConnection = raftConnection{serverPort, client}
			result <- &newConnection
		}
	}
}

func ConnectToRaftServers(myID ServerID, otherServers []ServerID) (*sync.Map, int) {
	var num = 0
	const connectionTimeout time.Duration = 300
	var establishedConnections sync.Map
	responseChan := make(chan *raftConnection)
	for i := 0; i < len(otherServers); i++ {
		go func(i int) {
			connectToRaftServer(otherServers[i], responseChan)
		}(i)
	}
	go func() {
		connectToRaftServer(myID, responseChan)
	}()
	for i := 0; i < len(otherServers)+1; i++ {
		select {
		case resp := <-responseChan:
			establishedConnections.Store((*resp).id, (*resp).connection)
			// Only keep track of number of connections to other servers
			if (*resp).id != myID {
				num++
			}
		case <-time.After(time.Second * connectionTimeout):
			log.Fatal("Timeout connecting to other nodes")
		}
	}
	return &establishedConnections, num
}

func startListeningServer(raftListener *RaftListener, port string) {
	rpc.Register(raftListener)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(listener, nil)
	fmt.Printf("Raft listener up on port %s \n", port)
}

func sendRequestVoteRPCs(opt *options, requestVoteArgs *RequestVoteArgs) {
	const electionTimeout time.Duration = 200
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		if id.(ServerID) == (*opt)._state.getID() {
			return true
		}
		// fmt.Println("Send requestVoteRPC: " + string(id))
		var requestVoteResponse RequestVoteResponse
		requestVoteCall := (*connection.(*rpc.Client)).Go("RaftListener.RequestVoteRPC", requestVoteArgs, &requestVoteResponse, nil)
		go func(opt *options, appendEntriesCall *rpc.Call, id ServerID) {
			select {
			case <-requestVoteCall.Done:
				(*opt).myRequestVoteResponseChan <- &requestVoteResponse
			case <-time.After(time.Second * electionTimeout):
				fmt.Println("RequestVoteRPC: Did not receive response from: " + string(id))
				// TODO: handle error
			}
		}(opt, requestVoteCall, id.(ServerID))
		return true
	})
}

/*
 * A server remains in Follower state as long as it receives valid
 * RPCs from a Leader or Candidate.
 */
func handleFollower(opt *options) {
	// fmt.Println("# Follower: handle turn")
	var electionTimeoutTimer = (*opt)._state.checkElectionTimeout()
	select {
	// Received message from client: respond with correct leader id
	case <-(*opt).msgChan:
		// fmt.Println("# Follower: received action")
		(*opt).msgResponseChan <- &ActionResponse{false, (*opt)._state.getCurrentLeader()}
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		// fmt.Println("# Follower: receive AppendEntriesRPC")
		(*opt)._state.stopElectionTimeout()
		(*opt).appendEntriesResponseChan <- (*opt)._state.handleAppendEntries(appEntrArgs)
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// fmt.Println("# Follower: receive RequestVoteRPC")
		(*opt)._state.stopElectionTimeout()
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	case <-(*electionTimeoutTimer).C:
		// fmt.Println("# Follower: election timeout")
		(*opt)._state.stopElectionTimeout()
		(*opt)._state.startElection()
		// Issue requestvoterpc in parallel to other servers
		var requestVoteArgs = (*opt)._state.prepareRequestVoteRPC()
		sendRequestVoteRPCs(opt, requestVoteArgs)
	}
}

func handleCandidate(opt *options) {
	// fmt.Println("## Candidate: handle current turn")
	var electionTimeoutTimer = (*opt)._state.checkElectionTimeout()
	select {
	// Received message from client: respond with correct leader id
	case <-(*opt).msgChan:
		// fmt.Println("## Candidate: received action")
		(*opt).msgResponseChan <- &ActionResponse{false, (*opt)._state.getCurrentLeader()}
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		// fmt.Println("## Candidate: receive AppendEntriesRPC")
		// Election timeout is stopped in handleAppendEntries if necessary
		(*opt).appendEntriesResponseChan <- (*opt)._state.handleAppendEntries(appEntrArgs)
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// fmt.Println("## Candidate: receive RequestVoteRPC")
		// If another candidate asks for a vote the logic doesn't change
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	// Receive a response to an issued RequestVoteRPC
	case reqVoteResponse := <-(*opt).myRequestVoteResponseChan:
		var currentVotes = (*opt)._state.updateElection(reqVoteResponse)
		// fmt.Printf("## Candidate: Received response to RequestVoteRPC, current votes: %d \n", currentVotes)
		// Check if a majority of votes was received
		if currentVotes > ((*opt).numberOfConnections+1)/2 {
			(*opt)._state.stopElectionTimeout()
			(*opt)._state.winElection()
			// Immediately send hearthbeat to every follower to establish leadership
			sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
		}
	case <-(*electionTimeoutTimer).C:
		// fmt.Println("## Candidate: Hit election timeout")
		(*opt)._state.stopElectionTimeout()
		// Too much time has passed with no leader or response, start anew
		(*opt)._state.startElection()
		// Issue requestvoterpc in parallel to other servers
		var requestVoteArgs = (*opt)._state.prepareRequestVoteRPC()
		sendRequestVoteRPCs(opt, requestVoteArgs)
	}
}

func sendAppendEntriesRPCs(opt *options, argsFunction func(ServerID) *AppendEntriesArgs) {
	const appendEntriesTimeout time.Duration = 200
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		if id.(ServerID) == (*opt)._state.getID() {
			return true
		}
		// fmt.Println("Send appendEntriesRPC: " + string(id))
		var appendEntriesResponse AppendEntriesResponse
		var appendEntriesArgs = argsFunction(id.(ServerID))
		appendEntriesCall := (*connection.(*rpc.Client)).Go("RaftListener.AppendEntriesRPC", appendEntriesArgs, &appendEntriesResponse, nil)
		go func(opt *options, appendEntriesCall *rpc.Call, id ServerID) {
			select {
			case <-appendEntriesCall.Done:
				(*opt).myAppendEntriesResponseChan <- &appendEntriesResponse
			case <-time.After(time.Millisecond * appendEntriesTimeout):
				fmt.Println("AppendEntriesRPC: Did not receive response from: " + string(id))
				// TODO: handle error
			}
		}(opt, appendEntriesCall, id.(ServerID))
		return true
	})
}

func handleLeader(opt *options) {
	// fmt.Println("### Leader: handle turn")
	const hearthbeatTimeout time.Duration = 20
	select {
	// Received message from client
	case msg := <-(*opt).msgChan:
		// fmt.Println("### Leader: receive action message")
		(*opt)._state.addNewLog(msg)
		sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
		// TODO: handle response only when message is committed
		(*opt).msgResponseChan <- &ActionResponse{true, (*opt)._state.getCurrentLeader()}
	// Handle responses to AppendEntries
	case appendEntriesResponse := <-(*opt).myAppendEntriesResponseChan:
		// fmt.Println("### Leader: receive response to append entries rpc")
		(*opt)._state.handleAppendEntriesResponse(appendEntriesResponse)
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// fmt.Println("### Leader: receive request vote rpc")
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		// fmt.Println("### Leader: receive append entries rpc")
		(*opt)._state.handleAppendEntries(appEntrArgs)
	// Receive a response to a (previously) issued RequestVoteRPC
	// Do nothing, just flush the channel
	case <-(*opt).myRequestVoteResponseChan:
		// fmt.Println("### Leader: receive votes issued previously")
	case <-time.After(time.Millisecond * hearthbeatTimeout):
		// fmt.Println("### Leader: hearthbeat")
		sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
	}
	// Check if leader should store new commits
	(*opt)._state.checkCommits()
}

func applyLog(opt *options, log RaftLog) {
	if (*opt).mode == "Client" {
		(*opt).actionChan <- log.Log
	}
	// TODO?: respond to client (maybe not necessary, being this a game)
}

func checkLogsToApply(opt *options) {
	var idxToExec = (*opt)._state.updateLastApplied()
	if idxToExec > 0 {
		applyLog(opt, (*opt)._state.getLog(idxToExec))
	}
}

func run(opt *options) {
	for {
		// First check if there are logs to apply to the state machine
		checkLogsToApply(opt)
		switch (*opt)._state.getState() {
		case Follower:
			handleFollower(opt)
		case Candidate:
			handleCandidate(opt)
		case Leader:
			handleLeader(opt)
		}
	}

}
