package raft

import (
	"fmt"
	"go_raft/engine"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type RaftConnectionResponse struct {
	Id         ServerID
	Connection *rpc.Client
}

type RaftConnection struct {
	Connection *rpc.Client
	Old        bool
	New        bool
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
	// This is used to receive RequestVoteRPC arguments from the other nodes (through the listener)
	requestVoteArgsChan chan *RequestVoteArgs
	// This is used to send RequestVoteRPC responses to the other nodes (through the listener)
	requestVoteResponseChan chan *RequestVoteResponse
	// This is used to get responses from remote nodes when sending a RequestVoteRPC
	myRequestVoteResponseChan chan *RequestVoteResponse
	// This is used to receive InstallSnapshotRPC arguments from the other nodes (through the listener)
	installSnapshotArgsChan chan *InstallSnapshotArgs
	// This is used to send InstallSnapshotRPC responses to the other nodes (through the listener)
	installSnapshotResponseChan chan *InstallSnapshotResponse
	// This is used to get responses from remote nodes when sending a InstallSnapshotRPC
	myInstallSnapshotResponseChan chan *InstallSnapshotResponse
	// This is used to receive messages from clients RPC
	msgChan chan gameAction
	// This is used to send messages to the game engine
	actionChan             chan engine.GameLog
	snapshotRequestChan    chan bool
	snapshotResponseChan   chan engine.GameState
	snapshotInstallChan    chan engine.GameState
	connections            *sync.Map
	numberOfNewConnections int
	numberOfOldConnections int
	unvotingConnections    *sync.Map
	connectedChan          chan bool
	connected              bool
}

// Start function for server logic
func Start(mode string, port string, otherServers []ServerID, actionChan chan engine.GameLog, connectedChan chan bool, snapshotRequestChan chan bool, snapshotResponseChan chan engine.GameState, installSnapshotChan chan engine.GameState) *sync.Map {
	var newOptions = &options{
		mode,
		newState(port, otherServers, snapshotRequestChan, snapshotResponseChan),
		make(chan *AppendEntriesArgs),
		make(chan *AppendEntriesResponse),
		make(chan *AppendEntriesResponse),
		make(chan *RequestVoteArgs),
		make(chan *RequestVoteResponse),
		make(chan *RequestVoteResponse),
		make(chan *InstallSnapshotArgs),
		make(chan *InstallSnapshotResponse),
		make(chan *InstallSnapshotResponse),
		make(chan gameAction),
		actionChan,
		snapshotRequestChan,
		snapshotResponseChan,
		installSnapshotChan,
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

func ConnectToRaftServer(opt *options, serverPort ServerID, result chan *RaftConnectionResponse) {
	var startTime = time.Now().Unix()
	var connected = false
	for time.Now().Unix() < startTime+600 && !connected {
		client, err := rpc.DialHTTP("tcp", "127.0.0.1:"+string(serverPort))
		if err != nil {
			log.Warning("Error connecting to node: " + string(serverPort))
			time.Sleep(1 * time.Second)
		} else {
			connected = true
			if opt != nil {
				(*opt)._state.addNewServer(serverPort)
			}

			log.Info("Connected to node: " + string(serverPort))
			var newConnection = RaftConnectionResponse{serverPort, client}
			result <- &newConnection
		}
	}
}

func ConnectToRaftServers(opt *options, myID ServerID, otherServers []ServerID) (*sync.Map, int) {
	var num = 0
	const connectionTimeout time.Duration = 300
	var establishedConnections sync.Map
	responseChan := make(chan *RaftConnectionResponse)
	for i := 0; i < len(otherServers); i++ {
		go func(i int) {
			ConnectToRaftServer(opt, otherServers[i], responseChan)
		}(i)
	}
	go func() {
		ConnectToRaftServer(opt, myID, responseChan)
	}()
	for i := 0; i < len(otherServers)+1; i++ {
		select {
		case resp := <-responseChan:
			// Mark new connections as NEW
			var newConnection = RaftConnection{(*resp).Connection, false, true}
			establishedConnections.Store((*resp).Id, newConnection)
			if opt != nil {
				(*opt)._state.updateServerConfiguration((*resp).Id, [2]bool{false, true})
			}
			num++
			// Only keep track of number of connections to other servers
			//if (*resp).id != myID {
			//	num++
			//}
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
	log.Info(fmt.Sprintf("Raft listener up on port %s \n", port))
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
				log.Warning("RequestVoteRPC: Did not receive response from: " + string(id))
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
	// TODO: verify if we need rpc.Client.Close()
	for _, raftLog := range (*appEntrArgs).Entries {
		if raftLog.Type == Configuration {
			(*opt).numberOfNewConnections = raftLog.ConfigurationLog.NewCount
			(*opt).numberOfOldConnections = raftLog.ConfigurationLog.OldCount
			log.Debug(fmt.Sprintf("Updating node configuration (idx: %d, old: %d, new: %d)", raftLog.Idx, (*opt).numberOfOldConnections, (*opt).numberOfNewConnections))
			log.Debug(raftLog.ConfigurationLog.ConnMap)
			for id, state := range raftLog.ConfigurationLog.ConnMap {
				var connection, found = (*opt).connections.Load(id)
				if found {
					// If the node is already connected, just update status (OLD, NEW)
					var conn = connection.(RaftConnection)
					conn.Old = state[0]
					conn.New = state[1]
					// Check if node has been removed
					if conn.Old && !conn.New {
						log.Debug("Disconnected:", id)
						(*(*opt).connections).LoadAndDelete(id)
						(*opt)._state.removeServer(id)
					} else {
						(*(*opt).connections).Store(id, conn)
					}
				} else {
					// Otherwise we need to connect to the node
					responseChan := make(chan *RaftConnectionResponse)
					go ConnectToRaftServer(opt, id, responseChan)
					resp := <-responseChan
					var newConnection = RaftConnection{(*resp).Connection, false, true}
					(*opt)._state.updateServerConfiguration(id, [2]bool{false, true})
					(*(*opt).connections).Store(id, newConnection)
				}
			}
		}
	}
}

func installSnapshot(opt *options, installSnapshotArgs *InstallSnapshotArgs) {
	var installSnapshotResponse = (*opt)._state.handleInstallSnapshotRequest(installSnapshotArgs)
	if (*installSnapshotResponse).Success {
		(*opt).snapshotInstallChan <- (*installSnapshotArgs).Data
	}
	(*opt).installSnapshotResponseChan <- installSnapshotResponse
}

/*
 * A server remains in Follower state as long as it receives valid
 * RPCs from a Leader or Candidate.
 */
func handleFollower(opt *options) {
	log.Trace("# Follower: handle current turn")
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
		var response = (*opt)._state.handleAppendEntries(appEntrArgs)
		(*opt).appendEntriesResponseChan <- response
		if (*response).Success {
			checkNewConfigurations(opt, appEntrArgs)
		}
		// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// fmt.Println("# Follower: receive RequestVoteRPC")
		(*opt)._state.stopElectionTimeout()
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	// Receive a InstallSnapshotRPC
	case installSnapshotArgs := <-(*opt).installSnapshotArgsChan:
		installSnapshot(opt, installSnapshotArgs)
	case <-(*opt).connectedChan:
		(*opt).connected = true
	case <-(*electionTimeoutTimer).C:
		// fmt.Println("# Follower: election timeout")
		// Only start new elections if fully connected to the raft network
		if (*opt).mode == "Node" || (*opt).connected {
			(*opt)._state.stopElectionTimeout()
			(*opt)._state.startElection()
			// Issue requestvoterpc in parallel to other servers
			if countConnections(opt) > 1 {
				var requestVoteArgs = (*opt)._state.prepareRequestVoteRPC()
				sendRequestVoteRPCs(opt, requestVoteArgs)
			} else {
				(*opt)._state.winElection()
			}
		}
	// Do nothing, just flush the channel
	case <-(*opt).myRequestVoteResponseChan:
	case <-(*opt).myInstallSnapshotResponseChan:
	case <-(*opt).myAppendEntriesResponseChan:
	}
}

func checkVotesMajority(opt *options, newVotes int, oldVotes int) bool {
	log.Trace(fmt.Sprintf("Check majority: new %d/%d, old %d/%d", newVotes, (*opt).numberOfNewConnections, oldVotes, (*opt).numberOfOldConnections))
	var oldMajority = true
	var newMajority = newVotes > ((*opt).numberOfNewConnections)/2
	if (*opt).numberOfOldConnections > 0 {
		oldMajority = oldVotes > ((*opt).numberOfOldConnections)/2
	}
	return newMajority && oldMajority
}

func handleCandidate(opt *options) {
	log.Trace("## Candidate: handle current turn")
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
		var response = (*opt)._state.handleAppendEntries(appEntrArgs)
		(*opt).appendEntriesResponseChan <- response
		if (*response).Success {
			checkNewConfigurations(opt, appEntrArgs)
		}
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// fmt.Println("## Candidate: receive RequestVoteRPC")
		// If another candidate asks for a vote the logic doesn't change
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
		// Receive a InstallSnapshotRPC
	case installSnapshotArgs := <-(*opt).installSnapshotArgsChan:
		installSnapshot(opt, installSnapshotArgs)
	// Receive a response to an issued RequestVoteRPC
	case reqVoteResponse := <-(*opt).myRequestVoteResponseChan:
		log.Trace("Received RequestVoteRPC response from: ", (*reqVoteResponse).Id)
		var connection, _ = (*(*opt).connections).Load((*reqVoteResponse).Id)
		var conn = connection.(RaftConnection)
		var currentVotesOld, currentVotesNew = (*opt)._state.updateElection(reqVoteResponse, conn.Old, conn.New)
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
	// Do nothing, just flush the channel
	case <-(*opt).myInstallSnapshotResponseChan:
	case <-(*opt).myAppendEntriesResponseChan:
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
			log.Warning("AppendEntriesRPC: Did not receive response from: " + string(id))
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

func sendInstallSnapshotRPC(opt *options, unvoting bool, id ServerID) {
	log.Debug("Send install snapshot RPC")
	const installSnapshotTimeout time.Duration = 200
	var installSnapshotResponse InstallSnapshotResponse
	var installSnapshotArgs = (*opt)._state.prepareInstallSnapshotRPC()
	var connection interface{}
	if unvoting {
		connection, _ = (*opt).unvotingConnections.Load(id)
	} else {
		connection, _ = (*opt).connections.Load(id)
	}
	var raftConn = connection.(RaftConnection)

	installSnapshotCall := raftConn.Connection.Go("RaftListener.InstallSnapshotRPC", installSnapshotArgs, &installSnapshotResponse, nil)
	go func(opt *options, installSnapshotCall *rpc.Call, id ServerID) {
		select {
		case <-installSnapshotCall.Done:
			(*opt).myInstallSnapshotResponseChan <- &installSnapshotResponse
		case <-time.After(time.Millisecond * installSnapshotTimeout):
			log.Warning("InstallSnapshotRPC: Did not receive response from: " + string(id))
			// TODO: handle error
		}
	}(opt, installSnapshotCall, id)
}

func handleResponseToMessage(opt *options, chanApplied chan bool, chanResponse chan *ActionResponse) {
	const handleResponseTimeout = 500
	select {
	case <-chanApplied:
		chanResponse <- &ActionResponse{true, (*opt)._state.getCurrentLeader()}
	case <-time.After(time.Millisecond * handleResponseTimeout):
		log.Warning("Timeout waiting for action to be applied")
	}
}

func startConfigurationChange(opt *options, newID ServerID, add bool) (map[ServerID][2]bool, int, int) {
	log.Debug("Start configuration change")
	var newCount = 0
	var oldCount = 0
	var connectionMap = map[ServerID][2]bool{}
	// Remove new connection from unvoting connection list
	var newConnection RaftConnection
	if add {
		var conn, _ = (*(*opt).unvotingConnections).LoadAndDelete(newID)
		newConnection = conn.(RaftConnection)
	}

	// Mark all previous connections as OLD, NEW
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		var connID = id.(ServerID)
		var conn = connection.(RaftConnection)
		// If we are removing the connection don't mark it as NEW
		if add || connID != newID {
			conn.New = true
			newCount++
		} else {
			conn.New = false
		}
		conn.Old = true
		oldCount++
		(*(*opt).connections).Store(id, conn)
		connectionMap[id.(ServerID)] = [2]bool{conn.Old, conn.New}
		return true
	})
	if add {
		// Mark new connection as NEW
		(*(*opt).connections).Store(newID, RaftConnection{newConnection.Connection, false, true})
		connectionMap[newID] = [2]bool{false, true}
		newCount++
	}

	return connectionMap, oldCount, newCount
}

func finishConfigurationChange(opt *options, add bool) (map[ServerID][2]bool, int) {
	log.Debug("Finish configuration change")
	var newCount = 0
	var connectionMap = map[ServerID][2]bool{}
	var connectionsToRemove = []ServerID{}
	// Remove new connection from unvoting connection list
	// Mark all connections as NEW
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		var conn = connection.(RaftConnection)
		if conn.New {
			conn.Old = false
			connectionMap[id.(ServerID)] = [2]bool{false, true}
			newCount++
			(*(*opt).connections).Store(id, conn)
		} else {
			connectionsToRemove = append(connectionsToRemove, id.(ServerID))
		}
		return true
	})

	for _, id := range connectionsToRemove {
		log.Debug("Remove connection: " + id)
		var _, _ = (*(*opt).connections).LoadAndDelete(id)
	}
	return connectionMap, newCount
}

func convertID(pID engine.PlayerID) ServerID {
	return ServerID(fmt.Sprint(pID))
}

func handleLeader(opt *options) {
	log.Trace("### Leader: handle turn")
	const hearthbeatTimeout time.Duration = 20
	select {
	// Received message from client
	case act := <-(*opt).msgChan:
		if act.Msg.Action == engine.CONNECT {
			log.Debug("Received request to connect")
			// Connect to new node and add it to the unvotingConnections map
			responseChan := make(chan *RaftConnectionResponse)
			go ConnectToRaftServer(opt, convertID(act.Msg.Id), responseChan)
			resp := <-responseChan
			var newConnection = RaftConnection{(*resp).Connection, false, false}
			(*(*opt).unvotingConnections).Store((*resp).Id, newConnection)
			(*opt)._state.updateServerConfiguration((*resp).Id, [2]bool{false, false})
			// TODO this should be removed eventually
			(*opt)._state.updateNewServerResponseChans((*resp).Id, act.Msg.ChanApplied)
			go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
		} else if act.Msg.Action == engine.DISCONNECT {
			log.Debug("Received request to disconnect")
			connMap, oldCount, newCount := startConfigurationChange(opt, convertID(act.Msg.Id), false)
			(*opt)._state.addNewConfigurationLog(ConfigurationLog{convertID(act.Msg.Id), connMap, oldCount, newCount, nil})
			(*opt)._state.updateNewServerResponseChans(convertID(act.Msg.Id), act.Msg.ChanApplied)
			sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
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
		var matchIndex, snapshot = (*opt)._state.handleAppendEntriesResponse(appendEntriesResponse)

		// Check if unvoting member should be promoted to voting
		var _, found = (*opt).unvotingConnections.Load((*appendEntriesResponse).Id)

		if snapshot {
			sendInstallSnapshotRPC(opt, found, (*appendEntriesResponse).Id)
		}

		if found && matchIndex >= (*opt)._state.getCommitIndex() {
			(*opt)._state.updateServerConfiguration((*appendEntriesResponse).Id, [2]bool{false, true})
			connMap, oldCount, newCount := startConfigurationChange(opt, (*appendEntriesResponse).Id, true)
			(*opt)._state.addNewConfigurationLog(ConfigurationLog{(*appendEntriesResponse).Id, connMap, oldCount, newCount, nil})
			sendAppendEntriesRPCs(opt, (*opt)._state.getAppendEntriesArgs)
		}
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// fmt.Println("### Leader: receive request vote rpc")
		(*opt).requestVoteResponseChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
		// Receive a InstallSnapshotRPC
	case installSnapshotArgs := <-(*opt).installSnapshotArgsChan:
		installSnapshot(opt, installSnapshotArgs)
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		// fmt.Println("### Leader: receive append entries rpc")
		(*opt)._state.handleAppendEntries(appEntrArgs)
	// Receive a response to a issued InstallSnapshotRPC
	case installSnapshotResponse := <-(*opt).myInstallSnapshotResponseChan:
		(*opt)._state.handleInstallSnapshotResponse(installSnapshotResponse)
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
	// TODO remove player from game if disconnected
	if log.Type == Game {
		(*opt).actionChan <- log.Log
	}
	if (*opt)._state.getState() == Leader {
		if log.Type == Game {
			log.Log.ChanApplied <- true
		} else if log.Type == Configuration {
			// If a configuration change log is committed (OLD, NEW configuration), generate its closure
			if log.ConfigurationLog.OldCount > 0 {
				var add = log.ConfigurationLog.OldCount < log.ConfigurationLog.NewCount
				connMap, newCount := finishConfigurationChange(opt, add)
				var appliedChan = (*opt)._state.getNewServerResponseChan(log.ConfigurationLog.Id)
				(*opt)._state.addNewConfigurationLog(ConfigurationLog{log.ConfigurationLog.Id, connMap, 0, newCount, appliedChan})
			} else if log.ConfigurationLog.OldCount == 0 {
				if log.ConfigurationLog.ChanApplied != nil {
					log.ConfigurationLog.ChanApplied <- true
				}
				(*opt)._state.removeNewServerResponseChan(log.ConfigurationLog.Id)
			}
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
