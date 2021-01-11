package core

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type raftConnection struct {
	id         ServerID
	connection *rpc.Client
}

type options struct {
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
	// This is used to receive messages from clients
	msgChan     chan GameLog
	connections *map[ServerID]*rpc.Client
}

// Start function for server logic
func Start(port string, otherServers []ServerID) chan GameLog {
	msgChan := make(chan GameLog)
	var newOptions = &options{
		newState(port, otherServers),
		make(chan *AppendEntriesArgs),
		make(chan *AppendEntriesResponse),
		make(chan *AppendEntriesResponse),
		make(chan *RequestVoteArgs),
		make(chan *RequestVoteResponse),
		make(chan *RequestVoteResponse),
		msgChan,
		nil}
	var raftListener = initRaftListener(newOptions)
	startListeningServer(raftListener, port)
	newOptions.connections = connectToRaftServers(otherServers)
	go run(newOptions)
	return msgChan
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

func connectToRaftServers(otherServers []ServerID) *map[ServerID]*rpc.Client {
	const connectionTimeout time.Duration = 300
	establishedConnections := make(map[ServerID]*rpc.Client)
	responseChan := make(chan *raftConnection)
	for i := 0; i < len(otherServers); i++ {
		go func(i int) {
			connectToRaftServer(otherServers[i], responseChan)
		}(i)
	}
	for i := 0; i < len(otherServers); i++ {
		select {
		case resp := <-responseChan:
			establishedConnections[(*resp).id] = (*resp).connection
		case <-time.After(time.Second * connectionTimeout):
			log.Fatal("Timeout connecting to other nodes")
		}
	}
	return &establishedConnections
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

func sendRequestVoteRPCs(connections *map[ServerID]*rpc.Client, requestVoteArgs *RequestVoteArgs, rvr chan *RequestVoteResponse) {
	const electionTimeout time.Duration = 200
	for id := range *connections {
		go func(id ServerID) {
			fmt.Println("Send requestVoteRPC: " + string(id))
			var requestVoteResponse RequestVoteResponse
			requestVoteCall := (*connections)[id].Go("RaftListener.RequestVoteRPC", requestVoteArgs, &requestVoteResponse, nil)
			select {
			case <-requestVoteCall.Done:
				rvr <- &requestVoteResponse
			case <-time.After(time.Second * electionTimeout):
				fmt.Println("RequestVoteRPC: Did not receive response from: " + string(id))
				// TODO: handle error
			}
		}(id)
	}
}

/*
 * A server remains in Follower state as long as it receives valid
 * RPCs from a Leader or Candidate.
 */
func handleFollower(opt *options) {
	fmt.Println("# Follower: handle turn")
	var electionTimeoutTimer = (*opt)._state.checkElectionTimeout()
	select {
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		fmt.Println("# Follower: receive AppendEntriesRPC")
		(*opt)._state.stopElectionTimeout()
		(*opt).appendEntriesResponseChan <- (*opt)._state.handleAppendEntries(appEntrArgs)
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		fmt.Println("# Follower: receive RequestVoteRPC")
		(*opt)._state.stopElectionTimeout()
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	case <-(*electionTimeoutTimer).C:
		fmt.Println("# Follower: election timeout")
		(*opt)._state.stopElectionTimeout()
		(*opt)._state.startElection()
		// Issue requestvoterpc in parallel to other servers
		var requestVoteArgs = (*opt)._state.prepareRequestVoteRPC()
		sendRequestVoteRPCs((*opt).connections, requestVoteArgs, (*opt).myRequestVoteResponseChan)
	}
}

func handleCandidate(opt *options) {
	fmt.Println("## Candidate: handle current turn")
	var electionTimeoutTimer = (*opt)._state.checkElectionTimeout()
	select {
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		fmt.Println("## Candidate: receive AppendEntriesRPC")
		// Election timeout is stopped in handleAppendEntries if necessary
		(*opt).appendEntriesResponseChan <- (*opt)._state.handleAppendEntries(appEntrArgs)
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		fmt.Println("## Candidate: receive RequestVoteRPC")
		// If another candidate asks for a vote the logic doesn't change
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	// Receive a responsed to an issued RequestVoteRPC
	case reqVoteResponse := <-(*opt).myRequestVoteResponseChan:
		var currentVotes = (*opt)._state.updateElection(reqVoteResponse)
		fmt.Printf("## Candidate: Received response to RequestVoteRPC, current votes: %d \n", currentVotes)
		// Check if a majority of votes was received
		if currentVotes > (len(*(*opt).connections)+1)/2 {
			(*opt)._state.stopElectionTimeout()
			(*opt)._state.winElection()
			// Immediately send hearthbeat to every follower to establish leadership
			sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
		}
	case <-(*electionTimeoutTimer).C:
		fmt.Println("## Candidate: Hit election timeout")
		(*opt)._state.stopElectionTimeout()
		// Too much time has passed with no leader or response, start anew
		(*opt)._state.startElection()
		// Issue requestvoterpc in parallel to other servers
		var requestVoteArgs = (*opt)._state.prepareRequestVoteRPC()
		sendRequestVoteRPCs((*opt).connections, requestVoteArgs, (*opt).myRequestVoteResponseChan)
	}
}

func sendAppendEntriesRPCs(opt *options, argsFunction func(ServerID) *AppendEntriesArgs) {
	const appendEntriesTimeout time.Duration = 200
	for id := range *(*opt).connections {
		go func(id ServerID) {
			fmt.Println("Send appendEntriesRPC: " + string(id))
			var appendEntriesResponse AppendEntriesResponse
			var appendEntriesArgs = argsFunction(id)
			appendEntriesCall := (*(*opt).connections)[id].Go("RaftListener.AppendEntriesRPC", appendEntriesArgs, &appendEntriesResponse, nil)
			select {
			case <-appendEntriesCall.Done:
				(*opt).myAppendEntriesResponseChan <- &appendEntriesResponse
			case <-time.After(time.Millisecond * appendEntriesTimeout):
				fmt.Println("AppendEntriesRPC: Did not receive response from: " + string(id))
				// TODO: handle error
			}
		}(id)
	}
}

func handleLeader(opt *options) {
	const hearthbeatTimeout time.Duration = 20
	select {
	// Received message from client
	case msg := <-(*opt).msgChan:
		(*opt)._state.addNewLog(msg)
		// TODO: Respond after entry applied to state machine
		sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
	// Handle responses to AppendEntries
	case appendEntriesResponse := <-(*opt).myAppendEntriesResponseChan:
		(*opt)._state.handleAppendEntriesResponse(appendEntriesResponse)
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		(*opt)._state.handleAppendEntries(appEntrArgs)
	case <-time.After(time.Millisecond * hearthbeatTimeout):
		sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
	}
	// Check if leader should store new commits
	(*opt)._state.checkCommits()
}

func applyLog(log RaftLog) {
	fmt.Printf("Apply log: %d\n", log.Idx)
}

func checkLogsToApply(opt *options) {
	var idxToExec = (*opt)._state.updateLastApplied()
	if idxToExec > 0 {
		applyLog((*opt)._state.getLog(idxToExec))
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
