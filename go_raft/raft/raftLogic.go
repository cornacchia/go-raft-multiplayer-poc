package raft

import (
	"crypto"
	"crypto/rsa"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type RequestConnection struct {
	Id          ServerID
	Destination *sync.Map
}

type RaftConnectionResponse struct {
	Id         ServerID
	Connection *rpc.Client
}

type RaftConnection struct {
	Connection *rpc.Client
}

type gameAction struct {
	Msg          GameLog
	ChanResponse chan *ActionResponse
}

type configurationAction struct {
	Msg          ConfigurationLog
	ChanResponse chan *AddRemoveServerResponse
	Signature    []byte
}

type options struct {
	mode   string
	_state state
	// This is used to receive AppendEntriesRPC arguments from other nodes (through the listener)
	appendEntriesArgsChan chan *AppendEntriesArgs
	// This is used to send AppendEntriesRPC responses to the other nodes (through the listener)
	appendEntriesResponseChan      chan *AppendEntriesResponse
	otherAppendEntriesResponseChan chan *AppendEntriesResponse
	// This is used to get responses from remote nodes when sending an AppendEntriesRPC
	myAppendEntriesResponseChan chan *AppendEntriesResponse
	// This is used to receive RequestVoteRPC arguments from the other nodes (through the listener)
	requestVoteArgsChan chan requestVoteArgsWrapper
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
	msgChan  chan gameAction
	confChan chan configurationAction
	// This is used to send messages to the game engine
	actionChan               chan GameLog
	updateLeaderArgsChan     chan *UpdateLeaderArgs
	updateLeaderResponseChan chan *UpdateLeaderResponse
	snapshotRequestChan      chan bool
	snapshotResponseChan     chan []byte
	snapshotInstallChan      chan []byte
	connections              *sync.Map
	unvotingConnections      *sync.Map
	connectedChan            chan bool
	connected                bool
	requestConnectionChan    chan RequestConnection
	clientKeys               map[string]*rsa.PublicKey
	nodeKeys                 map[ServerID]*rsa.PublicKey
}

// Start function for server logic
func Start(mode string, port string, otherServers []ServerID, actionChan chan GameLog, connectedChan chan bool, snapshotRequestChan chan bool, snapshotResponseChan chan []byte, installSnapshotChan chan []byte, nodeKeys map[ServerID]*rsa.PublicKey, clientKeys map[string]*rsa.PublicKey, privateKey *rsa.PrivateKey) *sync.Map {
	var newOptions = &options{
		mode,
		newState(port, otherServers, snapshotRequestChan, snapshotResponseChan, installSnapshotChan, nodeKeys, clientKeys, privateKey),
		make(chan *AppendEntriesArgs),
		make(chan *AppendEntriesResponse),
		make(chan *AppendEntriesResponse),
		make(chan *AppendEntriesResponse),
		make(chan requestVoteArgsWrapper),
		make(chan *RequestVoteResponse),
		make(chan *RequestVoteResponse),
		make(chan *InstallSnapshotArgs),
		make(chan *InstallSnapshotResponse),
		make(chan *InstallSnapshotResponse),
		make(chan gameAction),
		make(chan configurationAction),
		actionChan,
		make(chan *UpdateLeaderArgs),
		make(chan *UpdateLeaderResponse),
		snapshotRequestChan,
		snapshotResponseChan,
		installSnapshotChan,
		nil,
		nil,
		connectedChan,
		len(otherServers) == 0,
		make(chan RequestConnection),
		clientKeys,
		nodeKeys}
	var raftListener = initRaftListener(newOptions)
	startListeningServer(raftListener, port)
	nodeConnections := ConnectToRaftServers(newOptions, newOptions._state.getID(), otherServers)
	newOptions.connections = nodeConnections
	newOptions.unvotingConnections = &sync.Map{}
	go ConnectionManager(newOptions, newOptions.requestConnectionChan)
	go run(newOptions)
	return nodeConnections
}

func ConnectionManager(opt *options, requestConnectionChan chan RequestConnection) {
	var ungoingConnections = make(map[ServerID]bool)
	responseChan := make(chan ServerID)
	for {
		select {
		case newConnReq := <-requestConnectionChan:
			if connecting, found := ungoingConnections[newConnReq.Id]; found {
				// Check if we are already connecting to this server
				if !connecting {
					if opt != nil {
						log.Trace("Raft - Connection manager, attempt connection to ", newConnReq.Id)
					} else {
						log.Trace("Main - Connection manager, attempt connection to ", newConnReq.Id)
					}
					ungoingConnections[newConnReq.Id] = true
					go EnsureConnectionToServer(opt, newConnReq.Id, newConnReq.Destination, responseChan)
				}
			} else {
				if opt != nil {
					log.Trace("Raft - Connection manager, attempt connection to ", newConnReq.Id)
				} else {
					log.Trace("Main - Connection manager, attempt connection to ", newConnReq.Id)
				}
				ungoingConnections[newConnReq.Id] = true
				go EnsureConnectionToServer(opt, newConnReq.Id, newConnReq.Destination, responseChan)
			}
		case connectedID := <-responseChan:
			ungoingConnections[connectedID] = false
		}
	}
}

func ConnectToRaftServer(opt *options, serverPort ServerID, result chan *RaftConnectionResponse) {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:"+string(serverPort))
	if err != nil {
		log.Warning("Error connecting to node: "+string(serverPort), " ", err)
		result <- &RaftConnectionResponse{serverPort, nil}
	} else {
		if opt != nil {
			(*opt)._state.addNewServer(serverPort)
			log.Info("Raft - Connected to node: " + string(serverPort))
		} else {
			log.Trace("Main - Connected to node: " + string(serverPort))
		}

		var newConnection = RaftConnectionResponse{serverPort, client}
		result <- &newConnection
	}
}

func ConnectToRaftServers(opt *options, myID ServerID, otherServers []ServerID) *sync.Map {
	const connectionTimeout time.Duration = 300
	var establishedConnections sync.Map
	responseChan := make(chan ServerID)

	go EnsureConnectionToServer(opt, myID, &establishedConnections, responseChan)
	for i := 0; i < len(otherServers); i++ {
		go EnsureConnectionToServer(opt, otherServers[i], &establishedConnections, responseChan)
	}

	for i := 0; i < len(otherServers)+1; i++ {
		select {
		case <-responseChan:
		case <-time.After(time.Second * connectionTimeout):
			log.Fatal("Timeout connecting to other nodes")
		}
	}
	return &establishedConnections
}

func CloseConnection(id ServerID, relevantConnections *sync.Map) {
	log.Trace("Close connection with server: ", id)
	if conn, found := (*relevantConnections).LoadAndDelete(id); found {
		var raftConn = conn.(RaftConnection)
		if raftConn.Connection != nil {
			raftConn.Connection.Close()
		}
	}
}

func EnsureConnectionToServer(opt *options, serverPort ServerID, relevantConnections *sync.Map, responseChan chan ServerID) {
	if opt != nil {
		log.Debug("Raft - Ensure connection to server: ", serverPort)
	} else {
		log.Debug("Main - Ensure connection to server: ", serverPort)
	}
	localRespChan := make(chan *RaftConnectionResponse)
	var connected = false
	CloseConnection(serverPort, relevantConnections)
	for !connected {
		go ConnectToRaftServer(opt, serverPort, localRespChan)
		resp := <-localRespChan
		if (*resp).Connection == nil {
			time.Sleep(time.Second * 1)
			if opt != nil {
				log.Trace("Raft - Ensure connection to server, try again: ", serverPort)
			}
		} else {
			var newConnection = RaftConnection{(*resp).Connection}
			relevantConnections.Store((*resp).Id, newConnection)
			connected = true
		}
	}
	responseChan <- serverPort
}

func startListeningServer(raftListener *RaftListener, port string) {
	rpc.Register(raftListener)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(listener, nil)
	log.Info("Raft listener up on port: ", port)
}

func sendRequestVoteRPCs(opt *options, requestVoteArgs *RequestVoteArgs) {
	const electionTimeout time.Duration = 150
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		if id.(ServerID) == (*opt)._state.getID() {
			return true
		}
		log.Info("Sending RequestVoteRPC: ", id)
		var requestVoteResponse RequestVoteResponse
		var raftConn = connection.(RaftConnection)
		requestVoteCall := raftConn.Connection.Go("RaftListener.RequestVoteRPC", requestVoteArgs, &requestVoteResponse, nil)
		go func(opt *options, requestVoteCall *rpc.Call, id ServerID) {
			select {
			case <-requestVoteCall.Done:
				if requestVoteResponse.Id == "" {
					log.Debug("RequestVoteRPC: Removing unresponsive connection ", id, " ", requestVoteResponse)
					CloseConnection(id, (*opt).connections)
					if (*opt)._state.serverInConfiguration(id) {
						(*opt).requestConnectionChan <- RequestConnection{id, (*opt).connections}
					}
				} else {
					(*opt).myRequestVoteResponseChan <- &requestVoteResponse
				}
			case <-time.After(time.Millisecond * electionTimeout):
				log.Warning("RequestVoteRPC: Did not receive response from: " + string(id))
			}
		}(opt, requestVoteCall, id.(ServerID))

		return true
	})
}

func addConnection(opt *options, id ServerID) {
	log.Trace("Add connection to server: ", id)
	if _, found := (*opt).connections.Load(id); !found {
		(*opt).requestConnectionChan <- RequestConnection{id, (*opt).connections}
	}
}

func checkNewConfigurations(opt *options, appEntrArgs *AppendEntriesArgs) {
	for _, raftLog := range (*appEntrArgs).Entries {
		if raftLog.Type == Configuration {
			if raftLog.ConfigurationLog.Add {
				addConnection(opt, raftLog.ConfigurationLog.Server)
			} else {
				CloseConnection(raftLog.ConfigurationLog.Server, (*opt).connections)
			}
		}
	}
}

func installSnapshot(opt *options, isa *InstallSnapshotArgs) {
	var installSnapshotResponse = (*opt)._state.handleInstallSnapshotRequest(isa)
	if (*installSnapshotResponse).Success {
		(*opt).snapshotInstallChan <- (*isa).Data
		for id, _ := range (*isa).ServerConfiguration {
			addConnection(opt, id)
		}
	}
	(*opt).installSnapshotResponseChan <- installSnapshotResponse
}

func handleClientMessages(opt *options) {
	for {
		act := <-(*opt).msgChan
		switch (*opt)._state.getState() {
		case Follower:
			// act.ChanResponse <- &ActionResponse{false, (*opt)._state.getCurrentLeader()}
			if act.Msg.ActionId > (*opt)._state.getClientLastActionApplied(ServerID(act.Msg.Id)) {
				(*opt)._state.addClientChanResponse(act.Msg.Id, act.Msg.ActionId, act.Msg.ChanApplied)
				go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
			} else {
				go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
				act.Msg.ChanApplied <- true
			}
		case Candidate:
			// act.ChanResponse <- &ActionResponse{false, (*opt)._state.getCurrentLeader()}
			if act.Msg.ActionId > (*opt)._state.getClientLastActionApplied(ServerID(act.Msg.Id)) {
				(*opt)._state.addClientChanResponse(act.Msg.Id, act.Msg.ActionId, act.Msg.ChanApplied)
				go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
			} else {
				go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
				act.Msg.ChanApplied <- true
			}
		case Leader:
			// Handle player game action (i.e. movement)
			if act.Msg.ActionId > (*opt)._state.getClientLastActionApplied(ServerID(act.Msg.Id)) {
				// In this case the action is new
				var ok = (*opt)._state.addNewGameLog(act.Msg)
				if ok {
					sendAppendEntriesRPCs(opt)
					go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
				}
			} else {
				// The action has already been applied, respond immediately (cft. Raft paper section 8 p.13)
				// Note: this only prevents actions to be applied twice, i.e. it will work with snapshots
				// because the counter will be updated naturally when new messages arrive
				go handleResponseToMessage(opt, act.Msg.ChanApplied, act.ChanResponse)
				act.Msg.ChanApplied <- true
			}
		}
	}
}

func handleConfigurationMessages(opt *options) {
	for {
		conf := <-(*opt).confChan
		switch (*opt)._state.getState() {
		case Follower:
			log.Trace("Follower: refuse connection: ", conf.Msg.Server, " new leader: ", (*opt)._state.getCurrentLeader())
			conf.ChanResponse <- &AddRemoveServerResponse{false, (*opt)._state.getCurrentLeader()}
		case Candidate:
			log.Trace("Candidate: refuse connection: ", conf.Msg.Server, " new leader: ", (*opt)._state.getCurrentLeader())
			conf.ChanResponse <- &AddRemoveServerResponse{false, (*opt)._state.getCurrentLeader()}
		case Leader:
			log.Trace("Raft - Received configuration message: ", conf.Msg)
			var hashed = GetConfigurationLogBytes(conf.Msg)
			err := rsa.VerifyPKCS1v15(((*opt).nodeKeys[conf.Msg.Server]), crypto.SHA256, hashed[:], conf.Signature)
			if err != nil {
				conf.ChanResponse <- &AddRemoveServerResponse{false, ""}
			} else {
				if conf.Msg.Add {
					// Check if this is a reconnection after a node failure
					if _, found := (*opt).connections.LoadAndDelete(conf.Msg.Server); found {
						(*opt)._state.removeServer(ServerID(conf.Msg.Server))
					}
					(*opt).requestConnectionChan <- RequestConnection{conf.Msg.Server, (*opt).unvotingConnections}
					// If it is a connection request store it for when the server will be up to date
					(*opt)._state.addNewUnvotingServer(conf)
				} else {
					// Otherwise immediately add it to the queue
					ok, conf := (*opt)._state.handleConfigurationRPC(conf)
					if ok {
						go handleResponseToConfigurationMessage(opt, conf.Msg.ChanApplied, conf.ChanResponse)
					}
				}
			}
		}
	}
}

func promoteUnvotingConnection(opt *options, id ServerID, conn RaftConnection) {
	connectionAction := (*opt)._state.removeUnvotingServerAction(id)
	ok, conf := (*opt)._state.handleConfigurationRPC(connectionAction)
	if ok {
		go handleResponseToConfigurationMessage(opt, conf.Msg.ChanApplied, conf.ChanResponse)
	}
	(*opt).connections.Store(id, conn)
}

func handleAppendEntriesRPCResponses(opt *options) {
	for {
		appendEntriesResponse := <-(*opt).myAppendEntriesResponseChan
		if (*opt)._state.getState() == Leader {
			// log.Debug("Rec AppendEntriesResponse ", appendEntriesResponse)
			var matchIndex, snapshot = (*opt)._state.handleAppendEntriesResponse(appendEntriesResponse)

			// Check if unvoting member should be promoted to voting
			var _, found = (*opt).unvotingConnections.Load((*appendEntriesResponse).Id)

			if snapshot {
				sendInstallSnapshotRPC(opt, found, (*appendEntriesResponse).Id)
			}
			if found && matchIndex >= (*opt)._state.getCommitIndex() {
				var conn, _ = (*opt).unvotingConnections.LoadAndDelete((*appendEntriesResponse).Id)
				promoteUnvotingConnection(opt, (*appendEntriesResponse).Id, conn.(RaftConnection))
			}
		}
	}
}

func handleInstallSnapshotResponses(opt *options) {
	for {
		installSnapshotResponse := <-(*opt).myInstallSnapshotResponseChan
		if (*opt)._state.getState() == Leader {
			var matchIndex = (*opt)._state.handleInstallSnapshotResponse(installSnapshotResponse)

			// Check if unvoting member should be promoted to voting
			var _, found = (*opt).unvotingConnections.Load((*installSnapshotResponse).Id)

			// Immediately promote to voting if snapshot installed correctly
			if found && matchIndex >= (*opt)._state.getCommitIndex() {
				var conn, _ = (*opt).unvotingConnections.LoadAndDelete((*installSnapshotResponse).Id)
				promoteUnvotingConnection(opt, (*installSnapshotResponse).Id, conn.(RaftConnection))
			}
		}
	}
}

func handleOtherAppendEntriesResponse(opt *options) {
	for {
		appendEntriesResponse := <-(*opt).otherAppendEntriesResponseChan
		var _, found = (*(*opt).connections).Load((*appendEntriesResponse).Id)
		if found {
			(*opt)._state.updateAppendEntriesResponseSituation(appendEntriesResponse)
		}
	}
}

func handleUpdateLeaderMessages(opt *options) {
	for {
		updateLeaderArgs := <-(*opt).updateLeaderArgsChan
		var currentLeader = (*opt)._state.getCurrentLeader()
		var success = false
		if (*updateLeaderArgs).LeaderID == currentLeader {
			(*opt)._state.ignoreHeartbeatsForThisTerm()
			success = true
		}
		(*opt).updateLeaderResponseChan <- &UpdateLeaderResponse{success, currentLeader}
	}
}

/*
 * A server remains in Follower state as long as it receives valid
 * RPCs from a Leader or Candidate.
 */
func handleFollower(opt *options) {
	var electionTimeoutTimer = (*opt)._state.checkElectionTimeout()
	select {
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		(*opt)._state.stopElectionTimeout()
		var response, signatureVerified = (*opt)._state.handleAppendEntries(appEntrArgs)
		(*opt).appendEntriesResponseChan <- response
		if response.Success && (*opt)._state.hasVoted() {
			(*opt)._state.clearPendingVotes()
		}
		if signatureVerified {
			checkNewConfigurations(opt, appEntrArgs)
		}
		if len(appEntrArgs.Entries) > 0 {
			broadcastAppendEntriesResponse(opt, response)
		}
		// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		var requestResponse = (*opt)._state.handleRequestToVote(reqVoteArgs)
		if requestResponse != nil {
			reqVoteArgs.respChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
		}
	// Receive a InstallSnapshotRPC
	case installSnapshotArgs := <-(*opt).installSnapshotArgsChan:
		installSnapshot(opt, installSnapshotArgs)
	case <-(*opt).connectedChan:
		(*opt).connected = true
	case <-(*electionTimeoutTimer).C:
		// Only start new elections if fully connected to the raft network
		(*opt)._state.stopElectionTimeout()
		if (*opt)._state.hasVoted() {
			(*opt)._state.sendPendingVotes()
		} else {
			if (*opt).mode == "Node" || (*opt).connected {
				(*opt)._state.startElection()
				// Issue requestvoterpc in parallel to other servers
				if (*opt)._state.countConnections() > 1 {
					var requestVoteArgs = (*opt)._state.prepareRequestVoteRPC()
					sendRequestVoteRPCs(opt, requestVoteArgs)
				} else {
					(*opt)._state.winElection()
				}
			}
		}

	// Do nothing, just flush the channel
	case <-(*opt).myRequestVoteResponseChan:
	}
}

func handleCandidate(opt *options) {
	var electionTimeoutTimer = (*opt)._state.checkElectionTimeout()
	select {
	case <-(*opt).connectedChan:
		(*opt).connected = true
	// Received message from client: respond with correct leader id
	case act := <-(*opt).msgChan:
		act.ChanResponse <- &ActionResponse{false, (*opt)._state.getCurrentLeader()}
		// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		// Election timeout is stopped in handleAppendEntries if necessary
		var response, signatureVerified = (*opt)._state.handleAppendEntries(appEntrArgs)
		(*opt).appendEntriesResponseChan <- response
		if response.Success && (*opt)._state.hasVoted() {
			(*opt)._state.clearPendingVotes()
		}
		if signatureVerified {
			checkNewConfigurations(opt, appEntrArgs)
		}
		if len(appEntrArgs.Entries) > 0 {
			broadcastAppendEntriesResponse(opt, response)
		}
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		// If another candidate asks for a vote the logic doesn't change
		var requestResponse = (*opt)._state.handleRequestToVote(reqVoteArgs)
		if requestResponse != nil {
			reqVoteArgs.respChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
		}
		// Receive a InstallSnapshotRPC
	case installSnapshotArgs := <-(*opt).installSnapshotArgsChan:
		installSnapshot(opt, installSnapshotArgs)
	// Receive a response to an issued RequestVoteRPC
	case reqVoteResponse := <-(*opt).myRequestVoteResponseChan:
		// log.Trace("Received RequestVoteRPC response from: ", (*reqVoteResponse).Id)
		if becomeLeader := (*opt)._state.updateElection(reqVoteResponse); becomeLeader {
			sendAppendEntriesRPCs(opt)
		}
	case <-(*electionTimeoutTimer).C:
		(*opt)._state.stopElectionTimeout()
		if (*opt)._state.hasVoted() {
			(*opt)._state.sendPendingVotes()
		} else {
			// Too much time has passed with no leader or response, start anew
			(*opt)._state.startElection()
			// Issue requestvoterpc in parallel to other servers
			var requestVoteArgs = (*opt)._state.prepareRequestVoteRPC()
			sendRequestVoteRPCs(opt, requestVoteArgs)
		}
	}
}

func appendEntriesRPCAction(opt *options, appendEntriesArgs *AppendEntriesArgs, appendEntriesTimeout time.Duration, id interface{}, connection interface{}, unvoting bool) bool {
	if len((*appendEntriesArgs).Entries) > 0 {
		log.Info("Sending AppendEntriesRPC: ", id, " ", (*appendEntriesArgs).Entries[0].Idx, " ", (*appendEntriesArgs).Entries[len((*appendEntriesArgs).Entries)-1].Idx)
	}
	var appendEntriesResponse AppendEntriesResponse
	var raftConn = connection.(RaftConnection)
	appendEntriesCall := raftConn.Connection.Go("RaftListener.AppendEntriesRPC", appendEntriesArgs, &appendEntriesResponse, nil)
	go func(opt *options, appendEntriesCall *rpc.Call, id ServerID) {
		select {
		case <-appendEntriesCall.Done:
			if appendEntriesResponse.Id == "" {
				log.Debug("AppendEntriesRPC: Removing unresponsive connection ", id, " - ", appendEntriesResponse)
				if unvoting {
					CloseConnection(id, (*opt).unvotingConnections)
					(*opt).requestConnectionChan <- RequestConnection{id, (*opt).unvotingConnections}
				} else {
					CloseConnection(id, (*opt).connections)
					if (*opt)._state.serverInConfiguration(id) {
						(*opt).requestConnectionChan <- RequestConnection{id, (*opt).connections}
					}
				}
			} else {
				(*opt).myAppendEntriesResponseChan <- &appendEntriesResponse
			}
		case <-time.After(time.Millisecond * appendEntriesTimeout):
			log.Warning("AppendEntriesRPC: Did not receive response from: " + string(id))
		}
	}(opt, appendEntriesCall, id.(ServerID))

	return true
}

func sendAppendEntriesRPCs(opt *options) {
	const appendEntriesTimeout time.Duration = 200
	// AppendEntriesRPCs are sent to both voting and unvoting connections
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		if id.(ServerID) == (*opt)._state.getID() {
			return true
		}
		var args = (*opt)._state.getAppendEntriesArgs(id.(ServerID))
		if args == nil {
			sendInstallSnapshotRPC(opt, false, id.(ServerID))
			return true
		}
		return appendEntriesRPCAction(opt, args, appendEntriesTimeout, id, connection, false)
	})
	(*(*opt).unvotingConnections).Range(func(id interface{}, connection interface{}) bool {
		if id.(ServerID) == (*opt)._state.getID() {
			return true
		}
		var args = (*opt)._state.getAppendEntriesArgs(id.(ServerID))
		if args == nil {
			sendInstallSnapshotRPC(opt, true, id.(ServerID))
			return true
		}
		return appendEntriesRPCAction(opt, args, appendEntriesTimeout, id, connection, true)
	})
}

func appendEntriesResponseAction(opt *options, resp *AppendEntriesResponse, appendEntriesTimeout time.Duration, id interface{}, connection interface{}, unvoting bool) bool {
	var response bool
	var raftConn = connection.(RaftConnection)
	appendEntriesCall := raftConn.Connection.Go("RaftListener.AppendEntriesResponseRPC", resp, &response, nil)

	go func(opt *options, appendEntriesCall *rpc.Call, id ServerID) {
		select {
		case <-appendEntriesCall.Done:
		case <-time.After(time.Millisecond * appendEntriesTimeout):
			log.Warning("AppendEntriesResponseRPC: Did not receive response from: " + string(id))
		}
	}(opt, appendEntriesCall, id.(ServerID))

	return true
}

func broadcastAppendEntriesResponse(opt *options, resp *AppendEntriesResponse) {
	const appendEntriesTimeout time.Duration = 200
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		if id.(ServerID) == (*opt)._state.getID() {
			return true
		}

		return appendEntriesResponseAction(opt, resp, appendEntriesTimeout, id, connection, false)
	})
	(*(*opt).unvotingConnections).Range(func(id interface{}, connection interface{}) bool {
		if id.(ServerID) == (*opt)._state.getID() {
			return true
		}

		return appendEntriesResponseAction(opt, resp, appendEntriesTimeout, id, connection, true)
	})
}

func sendInstallSnapshotRPC(opt *options, unvoting bool, id ServerID) {
	const installSnapshotTimeout time.Duration = 200
	var installSnapshotResponse InstallSnapshotResponse
	var installSnapshotArgs = (*opt)._state.prepareInstallSnapshotRPC()
	log.Info("Sending InstallSnapshotRPC: ", id, " ", (*installSnapshotArgs).LastIncludedIndex)
	var connection interface{}
	var found = false
	if unvoting {
		connection, found = (*opt).unvotingConnections.Load(id)
	} else {
		connection, found = (*opt).connections.Load(id)
	}
	if found {
		var raftConn = connection.(RaftConnection)
		installSnapshotCall := raftConn.Connection.Go("RaftListener.InstallSnapshotRPC", installSnapshotArgs, &installSnapshotResponse, nil)
		go func(opt *options, installSnapshotCall *rpc.Call, id ServerID) {
			select {
			case <-installSnapshotCall.Done:
				if installSnapshotResponse.Id == "" {
					log.Debug("InstallSnapshotRPC: Removing unresponsive connection ", id, " ", installSnapshotResponse)
					if unvoting {
						CloseConnection(id, (*opt).unvotingConnections)
						(*opt).requestConnectionChan <- RequestConnection{id, (*opt).unvotingConnections}
					} else {
						CloseConnection(id, (*opt).connections)
						if (*opt)._state.serverInConfiguration(id) {
							(*opt).requestConnectionChan <- RequestConnection{id, (*opt).connections}
						}
					}
				} else {
					(*opt).myInstallSnapshotResponseChan <- &installSnapshotResponse
				}
			case <-time.After(time.Millisecond * installSnapshotTimeout):
				log.Warning("InstallSnapshotRPC: Did not receive response from: " + string(id))
			}
		}(opt, installSnapshotCall, id)
	}
}

func handleResponseToMessage(opt *options, chanApplied chan bool, chanResponse chan *ActionResponse) {
	const handleResponseTimeout = 1000
	select {
	case <-chanApplied:
		chanResponse <- &ActionResponse{true, (*opt)._state.getCurrentLeader()}
	case <-time.After(time.Millisecond * handleResponseTimeout):
		log.Warning("Timeout waiting for action to be applied")
	}
}

func handleResponseToConfigurationMessage(opt *options, chanApplied chan bool, chanResponse chan *AddRemoveServerResponse) {
	const handleResponseTimeout = 500
	select {
	case <-chanApplied:
		chanResponse <- &AddRemoveServerResponse{true, (*opt)._state.getCurrentLeader()}
	case <-time.After(time.Millisecond * handleResponseTimeout):
		log.Warning("Timeout waiting for configuration to be applied")
	}
}

func handleLeader(opt *options) {
	const hearthbeatTimeout time.Duration = 20
	select {
	case <-(*opt).connectedChan:
		(*opt).connected = true
	// Receive an AppendEntriesRPC
	case appEntrArgs := <-(*opt).appendEntriesArgsChan:
		var response, signatureVerified = (*opt)._state.handleAppendEntries(appEntrArgs)
		(*opt).appendEntriesResponseChan <- response
		if signatureVerified {
			checkNewConfigurations(opt, appEntrArgs)
		}
		if len(appEntrArgs.Entries) > 0 {
			broadcastAppendEntriesResponse(opt, response)
		}
	// Receive a RequestVoteRPC
	case reqVoteArgs := <-(*opt).requestVoteArgsChan:
		reqVoteArgs.respChan <- (*opt)._state.handleRequestToVote(reqVoteArgs)
	// Receive a response to a (previously) issued RequestVoteRPC
	// Do nothing, just flush the channel
	case <-(*opt).myRequestVoteResponseChan:
	case <-time.After(time.Millisecond * hearthbeatTimeout):
		sendAppendEntriesRPCs(opt)
	}
	// Check if leader should store new commits
	// (*opt)._state.checkCommits()
}

func applyLog(opt *options, raftLog RaftLog) {
	log.Info("Raft apply log: ", raftLog.Idx, " ", raftLogToString(raftLog))
	if raftLog.Type == Game {
		(*opt).actionChan <- raftLog.Log
		(*opt)._state.updateClientLastActionApplied(ServerID(raftLog.Log.Id), raftLog.Log.ActionId)
	}
	if (*opt)._state.getState() == Leader {
		if raftLog.Type == Game && raftLog.Log.ChanApplied != nil {
			// What if we received these logs as followers i.e. in append entries RPC and THEN we became leader?
			raftLog.Log.ChanApplied <- true
		} else if raftLog.Type == Configuration {
			if raftLog.ConfigurationLog.ChanApplied != nil {
				raftLog.ConfigurationLog.ChanApplied <- true
			}
			(*opt)._state.unlockNextConfiguration()
			if !raftLog.ConfigurationLog.Add {
				CloseConnection(raftLog.ConfigurationLog.Server, (*opt).connections)
			}
		}
	} else {
		if raftLog.Type == Game {
			(*opt)._state.respondToClientChanResponse(raftLog.Log.Id, raftLog.Log.ActionId, true)
		}
	}
}

func checkLogsToApply(opt *options) {
	var idxToExec = (*opt)._state.updateLastApplied()
	for idxToExec >= 0 {
		applyLog(opt, (*opt)._state.getLog(idxToExec))
		idxToExec = (*opt)._state.updateLastApplied()
	}
}

func checkConfigurationsToStart(opt *options) {
	ok, conf := (*opt)._state.handleNextConfigurationChange()
	if ok {
		go handleResponseToConfigurationMessage(opt, conf.Msg.ChanApplied, conf.ChanResponse)
	}
}

func run(opt *options) {
	go handleClientMessages(opt)
	go handleAppendEntriesRPCResponses(opt)
	go handleInstallSnapshotResponses(opt)
	go handleOtherAppendEntriesResponse(opt)
	go handleUpdateLeaderMessages(opt)
	go handleConfigurationMessages(opt)
	for {
		// First check if there are logs to apply to the state machine
		(*opt)._state.updateCommitIndex()
		checkLogsToApply(opt)
		checkConfigurationsToStart(opt)
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
