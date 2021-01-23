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

type raftConnectionResponse struct {
	id         ServerID
	connection *rpc.Client
}

type RaftConnection struct {
	Connection *rpc.Client
	old        bool
	new        bool
}

type gameAction struct {
	Msg          engine.GameLog
	ChanResponse chan *ActionResponse
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
	msgChan chan gameAction
	// This is used to send messages to the game engine
	actionChan             chan engine.GameLog
	connections            *sync.Map
	numberOfNewConnections int
	numberOfOldConnections int
	unvotingConnections    *sync.Map
	connectedChan          chan bool
	connected              bool
}

// Start function for server logic
func Start(mode string, port string, otherServers []ServerID, actionChan chan engine.GameLog, connectedChan chan bool) *sync.Map {
	var newOptions = &options{
		mode,
		newState(port, otherServers),
		make(chan *AppendEntriesArgs),
		make(chan *AppendEntriesResponse),
		make(chan *AppendEntriesResponse),
		make(chan *RequestVoteArgs),
		make(chan *RequestVoteResponse),
		make(chan *RequestVoteResponse),
		make(chan gameAction),
		actionChan,
		nil,
		0,
		0,
		nil,
		connectedChan,
		len(otherServers) == 0}
	var raftListener = initRaftListener(newOptions)
	startListeningServer(raftListener, port)
	nodeConnections, num := ConnectToRaftServers(newOptions, newOptions._state.getID(), otherServers)
	newOptions.connections = nodeConnections
	newOptions.numberOfNewConnections = num
	newOptions.unvotingConnections = &sync.Map{}
	go run(newOptions)
	return nodeConnections
}

func connectToRaftServer(opt *options, serverPort ServerID, result chan *raftConnectionResponse) {
	var startTime = time.Now().Unix()
	var connected = false
	for time.Now().Unix() < startTime+600 && !connected {
		client, err := rpc.DialHTTP("tcp", "127.0.0.1:"+string(serverPort))
		if err != nil {
			fmt.Println("Error connecting to node: " + string(serverPort))
			time.Sleep(1 * time.Second)
		} else {
			connected = true
			if opt != nil {
				(*opt)._state.addNewServer(serverPort)
			}

			fmt.Println("Connected to node: " + string(serverPort))
			var newConnection = raftConnectionResponse{serverPort, client}
			result <- &newConnection
		}
	}
}

func ConnectToRaftServers(opt *options, myID ServerID, otherServers []ServerID) (*sync.Map, int) {
	var num = 0
	const connectionTimeout time.Duration = 300
	var establishedConnections sync.Map
	responseChan := make(chan *raftConnectionResponse)
	for i := 0; i < len(otherServers); i++ {
		go func(i int) {
			connectToRaftServer(opt, otherServers[i], responseChan)
		}(i)
	}
	go func() {
		connectToRaftServer(opt, myID, responseChan)
	}()
	for i := 0; i < len(otherServers)+1; i++ {
		select {
		case resp := <-responseChan:
			// Mark new connections as NEW
			var newConnection = RaftConnection{(*resp).connection, false, true}
			establishedConnections.Store((*resp).id, newConnection)
			if opt != nil {
				(*opt)._state.updateServerConfiguration((*resp).id, [2]bool{false, true})
			}
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
		var raftConn = connection.(RaftConnection)
		requestVoteCall := raftConn.Connection.Go("RaftListener.RequestVoteRPC", requestVoteArgs, &requestVoteResponse, nil)
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

func countConnections(opt *options) int {
	return (*opt).numberOfNewConnections + (*opt).numberOfOldConnections
}

func checkNewConfigurations(opt *options, appEntrArgs *AppendEntriesArgs) {
	for _, log := range (*appEntrArgs).Entries {
		if log.Type == Configuration {
			(*opt).numberOfNewConnections = log.ConfigurationLog.NewCount
			(*opt).numberOfOldConnections = log.ConfigurationLog.OldCount
			for id, state := range log.ConfigurationLog.ConnMap {
				var connection, found = (*opt).connections.Load(id)
				if found {
					// If the node is already connected, just update status (OLD, NEW)
					var conn = connection.(RaftConnection)
					conn.old = state[0]
					conn.new = state[1]
					(*(*opt).connections).Store(id, conn)
				} else {
					// Otherwise we need to connect to the node
					responseChan := make(chan *raftConnectionResponse)
					connectToRaftServer(opt, id, responseChan)
					resp := <-responseChan
					var newConnection = RaftConnection{(*resp).connection, false, true}
					(*opt)._state.updateServerConfiguration((*resp).id, [2]bool{false, true})
					(*(*opt).connections).Store(id, newConnection)
				}
			}
		}
	}
}

/*
 * A server remains in Follower state as long as it receives valid
 * RPCs from a Leader or Candidate.
 */
func handleFollower(opt *options) {
	// fmt.Println("# Follower: handle current turn")
	var electionTimeoutTimer = (*opt)._state.checkElectionTimeout()
	select {
	// Received message from client: respond with correct leader id
	case act := <-(*opt).msgChan:
		// fmt.Println("# Follower: received action")
		act.ChanResponse <- &ActionResponse{false, (*opt)._state.getCurrentLeader()}
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		// fmt.Println("# Follower: receive AppendEntriesRPC")
		(*opt)._state.stopElectionTimeout()
		(*opt).appendEntriesResponseChan <- (*opt)._state.handleAppendEntries(appEntrArgs)
		checkNewConfigurations(opt, appEntrArgs)
		// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// fmt.Println("# Follower: receive RequestVoteRPC")
		(*opt)._state.stopElectionTimeout()
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	case <-(*opt).connectedChan:
		(*opt).connected = true
	case <-(*electionTimeoutTimer).C:
		// fmt.Println("# Follower: election timeout")
		// Only start new elections if fully connected to the raft network
		if (*opt).mode == "Node" || (*opt).connected {
			(*opt)._state.stopElectionTimeout()
			(*opt)._state.startElection()
			// Issue requestvoterpc in parallel to other servers
			if countConnections(opt) > 0 {
				var requestVoteArgs = (*opt)._state.prepareRequestVoteRPC()
				sendRequestVoteRPCs(opt, requestVoteArgs)
			} else {
				(*opt)._state.winElection()
			}
		}
	}
}

func checkVotesMajority(opt *options, newVotes int, oldVotes int) bool {
	var oldMajority = true
	var newMajority = newVotes > ((*opt).numberOfNewConnections+1)/2
	if (*opt).numberOfOldConnections > 0 {
		oldMajority = oldVotes > ((*opt).numberOfOldConnections+1)/2
	}
	return newMajority && oldMajority
}

func handleCandidate(opt *options) {
	// fmt.Println("## Candidate: handle current turn")
	var electionTimeoutTimer = (*opt)._state.checkElectionTimeout()
	select {
	// Received message from client: respond with correct leader id
	case act := <-(*opt).msgChan:
		// fmt.Println("## Candidate: received action")
		act.ChanResponse <- &ActionResponse{false, (*opt)._state.getCurrentLeader()}
		// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		// fmt.Println("## Candidate: receive AppendEntriesRPC")
		// Election timeout is stopped in handleAppendEntries if necessary
		(*opt).appendEntriesResponseChan <- (*opt)._state.handleAppendEntries(appEntrArgs)
		checkNewConfigurations(opt, appEntrArgs)
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// fmt.Println("## Candidate: receive RequestVoteRPC")
		// If another candidate asks for a vote the logic doesn't change
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	// Receive a response to an issued RequestVoteRPC
	case reqVoteResponse := <-(*opt).myRequestVoteResponseChan:
		var connection, _ = (*(*opt).connections).Load((*reqVoteResponse).Id)
		var conn = connection.(RaftConnection)
		var currentVotesNew, currentVotesOld = (*opt)._state.updateElection(reqVoteResponse, conn.old, conn.new)
		// fmt.Printf("## Candidate: Received response to RequestVoteRPC, current votes: %d \n", currentVotes)
		// Check if a majority of votes was received
		if checkVotesMajority(opt, currentVotesNew, currentVotesOld) {
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

func appendEntriesRPCAction(opt *options, argsFunction func(ServerID) *AppendEntriesArgs, appendEntriesTimeout time.Duration, id interface{}, connection interface{}) bool {
	if id.(ServerID) == (*opt)._state.getID() {
		return true
	}
	// fmt.Println("Send appendEntriesRPC: " + string(id))
	var appendEntriesResponse AppendEntriesResponse
	var appendEntriesArgs = argsFunction(id.(ServerID))
	var raftConn = connection.(RaftConnection)
	appendEntriesCall := raftConn.Connection.Go("RaftListener.AppendEntriesRPC", appendEntriesArgs, &appendEntriesResponse, nil)
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
}

func sendAppendEntriesRPCs(opt *options, argsFunction func(ServerID) *AppendEntriesArgs) {
	const appendEntriesTimeout time.Duration = 200
	// AppendEntriesRPCs are sent to both voting and unvoting connections
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		return appendEntriesRPCAction(opt, argsFunction, appendEntriesTimeout, id, connection)
	})
	(*(*opt).unvotingConnections).Range(func(id interface{}, connection interface{}) bool {
		return appendEntriesRPCAction(opt, argsFunction, appendEntriesTimeout, id, connection)
	})
}

func handleResponseToMessage(opt *options, chanApplied chan bool, chanResponse chan *ActionResponse) {
	const handleResponseTimeout = 500
	select {
	case <-chanApplied:
		chanResponse <- &ActionResponse{true, (*opt)._state.getCurrentLeader()}
	case <-time.After(time.Millisecond * handleResponseTimeout):
		fmt.Println("Timeout waiting for action to be applied")
	}
}

func startConfigurationChange(opt *options, newID ServerID) (map[ServerID][2]bool, int, int) {
	var newCount = 0
	var oldCount = 0
	var connectionMap = map[ServerID][2]bool{}
	// Remove new connection from unvoting connection list
	var newConnection, _ = (*(*opt).unvotingConnections).LoadAndDelete(newID)
	// Mark all previous connections as OLD, NEW
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		var conn = connection.(RaftConnection)
		conn.old = true
		conn.new = true
		(*(*opt).connections).Store(id, conn)
		newCount++
		oldCount++
		connectionMap[id.(ServerID)] = [2]bool{true, true}
		return true
	})
	// Mark new connection as NEW
	(*(*opt).connections).Store(newID, RaftConnection{newConnection.(RaftConnection).Connection, false, true})
	connectionMap[newID] = [2]bool{false, true}
	oldCount++
	return connectionMap, oldCount, newCount
}

func finishConfigurationChange(opt *options) (map[ServerID][2]bool, int) {
	var newCount = 0
	var connectionMap = map[ServerID][2]bool{}
	// Remove new connection from unvoting connection list
	// Mark all connections as NEW
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		var conn = connection.(RaftConnection)
		conn.old = false
		conn.new = true
		(*(*opt).connections).Store(id, conn)
		newCount++
		connectionMap[id.(ServerID)] = [2]bool{false, true}
		return true
	})
	return connectionMap, newCount
}

func convertID(pID engine.PlayerID) ServerID {
	return ServerID(fmt.Sprint(pID))
}

func handleLeader(opt *options) {
	// fmt.Println("### Leader: handle turn")
	const hearthbeatTimeout time.Duration = 20
	select {
	// Received message from client
	case act := <-(*opt).msgChan:
		//fmt.Println("### Leader: receive action message")
		if act.Msg.Action == engine.CONNECT {
			// Connect to new node and add it to the unvotingConnections map
			responseChan := make(chan *raftConnectionResponse)
			go connectToRaftServer(opt, convertID(act.Msg.Id), responseChan)
			resp := <-responseChan
			var newConnection = RaftConnection{(*resp).connection, false, false}
			(*(*opt).unvotingConnections).Store((*resp).id, newConnection)
			(*opt)._state.updateServerConfiguration((*resp).id, [2]bool{false, false})
			// TODO this should be removed eventually
			(*opt)._state.updateNewServerResponseChans((*resp).id, act.Msg.ChanApplied)
			go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
		} else {
			// Handle player game action (i.e. movement)
			(*opt)._state.addNewGameLog(act.Msg)
			sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
			go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
		}
	// Handle responses to AppendEntries
	case appendEntriesResponse := <-(*opt).myAppendEntriesResponseChan:
		// fmt.Println("### Leader: receive response to append entries rpc")
		var matchIndex = (*opt)._state.handleAppendEntriesResponse(appendEntriesResponse)
		// Check if unvoting member should be promoted to voting
		var _, found = (*opt).unvotingConnections.Load((*appendEntriesResponse).Id)
		if found && matchIndex == (*opt)._state.getCommitIndex() {
			connMap, oldCount, newCount := startConfigurationChange(opt, (*appendEntriesResponse).Id)
			(*opt)._state.addNewConfigurationLog(ConfigurationLog{(*appendEntriesResponse).Id, connMap, oldCount, newCount, nil})
			sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
		}
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
	if (*opt).mode == "Client" && log.Type == Game {
		(*opt).actionChan <- log.Log
	}
	if (*opt)._state.getState() == Leader {
		if log.Type == Game {
			log.Log.ChanApplied <- true
		}
		// If a configuration change log is committed (OLD, NEW configuration), generate its closure
		if log.Type == Configuration && log.ConfigurationLog.OldCount > 0 {
			connMap, newCount := finishConfigurationChange(opt)
			var appliedChan = (*opt)._state.getNewServerResponseChan(log.ConfigurationLog.Id)
			(*opt)._state.addNewConfigurationLog(ConfigurationLog{log.ConfigurationLog.Id, connMap, 0, newCount, appliedChan})
			sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
		}
		if log.Type == Configuration && log.ConfigurationLog.OldCount == 0 {
			log.ConfigurationLog.ChanApplied <- true
		}
	}
}

func checkLogsToApply(opt *options) {
	var idxToExec = (*opt)._state.updateLastApplied()
	for idxToExec > 0 {
		applyLog(opt, (*opt)._state.getLog(idxToExec))
		idxToExec = (*opt)._state.updateLastApplied()
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
