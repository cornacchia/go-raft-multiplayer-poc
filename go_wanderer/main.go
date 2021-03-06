package main

import (
	"encoding/json"
	"go_raft/raft"
	"go_wanderer/engine"
	"go_wanderer/ui"
	"math/rand"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

type options struct {
	actionChan             chan engine.GameLog
	configurationChan      chan bool
	firstLeader            raft.ServerID
	connections            *sync.Map
	otherServers           []raft.ServerID
	id                     raft.ServerID
	connectedChan          chan bool
	disconnectedChan       chan bool
	mode                   string
	requestConnectionChan  chan raft.RequestConnection
	requestNewServerIDChan chan bool
	getNewServerIDChan     chan raft.ServerID
	removedFromGameChan    chan bool
}

func checkError(err error) {
	if err != nil {
		log.Error("Error: ", err)
	}
}

func getNowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func addToKnownServers(opt *options, id raft.ServerID) {
	var shouldAdd = true
	for _, val := range (*opt).otherServers {
		if val == id {
			shouldAdd = false
			break
		}
	}
	if shouldAdd {
		(*opt).otherServers = append((*opt).otherServers, id)
	}
}

func connectionPool(opt *options) {
	var currentServer = -1
	for {
		<-(*opt).requestNewServerIDChan
		if len((*opt).otherServers) > 0 {
			currentServer = (currentServer + 1) % len((*opt).otherServers)
			(*opt).getNewServerIDChan <- (*opt).otherServers[currentServer]
		} else {
			(*opt).getNewServerIDChan <- (*opt).id
		}
	}
}

func handleCurrentTurn(currentTurnEngineChan chan int, currentTurnUIChan chan int) {
	var currentTurn = 0
	for {
		select {
		case newTurn := <-currentTurnEngineChan:
			if newTurn > currentTurn {
				currentTurn = newTurn
				log.Info("New turn: ", currentTurn)
			}
		case <-currentTurnUIChan:
			currentTurnUIChan <- currentTurn
		}
	}
}

func handleActionResponse(call *rpc.Call, response *raft.ActionResponse, changeConnectionChan chan raft.ServerID, msg engine.GameLog, timestamp int64, currentConnection raft.ServerID, opt *options, actionDoneChannel chan bool) {
	var waitTime time.Duration = 1000
	select {
	case <-call.Done:
		if !(*response).Applied {
			log.Trace("Main - Action not applied ", currentConnection, " - ", (*response))
			if (*response).LeaderID != "" {
				addToKnownServers(opt, (*response).LeaderID)
				changeConnectionChan <- (*response).LeaderID
			} else {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
				changeConnectionChan <- (*opt).id
			}
			actionDoneChannel <- false
		} else {
			if (*opt).mode == "Test" {
				var now = getNowMs()
				log.Info("Main - Action time: ", (now - timestamp), " - ", msg.ActionId)
			}
			actionDoneChannel <- true
		}
	case <-time.After(time.Millisecond * waitTime):
		if (*opt).mode == "Test" {
			log.Info("Main - Action dropped - ", currentConnection, " - ", msg.ActionId)
		}
		changeConnectionChan <- ""
		actionDoneChannel <- false
	}
}

func handleConfigurationResponse(call *rpc.Call, response *raft.AddRemoveServerResponse, changeConnectionChan chan raft.ServerID, msg bool, currentConnection raft.ServerID, opt *options) {
	var waitTime time.Duration = 5000
	if call == nil {
		time.Sleep(time.Millisecond * 300)
		(*opt).configurationChan <- msg
		return
	}
	select {
	case <-call.Done:
		if !(*response).Success {
			log.Trace("Main - Connection not applied ", currentConnection, " - ", (*response))
			if (*response).LeaderID != "" {
				addToKnownServers(opt, (*response).LeaderID)
				changeConnectionChan <- (*response).LeaderID
			} else {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
				changeConnectionChan <- (*opt).id
			}
			time.Sleep(time.Millisecond * 300)
			// Send again
			(*opt).configurationChan <- msg
		} else {
			if msg {
				(*opt).connectedChan <- true
			} else {
				(*opt).disconnectedChan <- true
			}
		}
	case <-time.After(time.Millisecond * waitTime):
		if (*opt).mode == "Test" {
			log.Info("Main - Connection dropped - ", currentConnection)
		}
		changeConnectionChan <- ""
		(*opt).configurationChan <- msg
	}
}

func manageActions(opt *options) {
	currentConnection := (*opt).firstLeader
	changeConnectionChan := make(chan raft.ServerID)
	var actions []engine.GameLog
	var clearToSend = true
	actionDoneChannel := make(chan bool)
	for {
		select {
		case msg := <-(*opt).actionChan:
			if len(actions) < 32 {
				actions = append(actions, msg)
			} else {
				log.Debug("Too many actions, drop one")
			}
		case msg := <-(*opt).configurationChan:
			var configurationResponse raft.AddRemoveServerResponse
			var configurationArgs = raft.AddRemoveServerArgs{(*opt).id, msg}
			var conn, found = (*(*opt).connections).Load(currentConnection)
			if !found {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
				go handleConfigurationResponse(nil, nil, changeConnectionChan, msg, currentConnection, opt)
				(*opt).requestNewServerIDChan <- true
				currentConnection = <-(*opt).getNewServerIDChan
			} else {
				log.Info("Main - Send connection request to ", currentConnection)
				var raftConn = conn.(raft.RaftConnection)
				actionCall := raftConn.Connection.Go("RaftListener.AddRemoveServerRPC", &configurationArgs, &configurationResponse, nil)
				go handleConfigurationResponse(actionCall, &configurationResponse, changeConnectionChan, msg, currentConnection, opt)
			}
		case newServerID := <-changeConnectionChan:
			if newServerID == "" {
				(*opt).requestNewServerIDChan <- true
				currentConnection = <-(*opt).getNewServerIDChan
			} else {
				currentConnection = newServerID
			}

			var _, found = (*(*opt).connections).Load(currentConnection)
			if !found {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
			}
		case done := <-actionDoneChannel:
			clearToSend = true
			if done && len(actions) > 0 {
				if actions[0].Action.Action == engine.DISCONNECT {
					(*opt).removedFromGameChan <- true
				}
				copy(actions, actions[1:])
				actions = actions[:len(actions)-1]
			}
		case <-time.After(time.Millisecond * 10):
			if clearToSend && len(actions) > 0 {
				clearToSend = false
				var msg = actions[0]
				log.Debug("Main - start sending: ", msg.ActionId)
				var timestamp = getNowMs()
				var actionResponse raft.ActionResponse
				var jsonAction, _ = json.Marshal(msg.Action)
				var actionArgs = raft.ActionArgs{string(msg.Id), msg.ActionId, msg.Type, jsonAction}
				var conn, found = (*(*opt).connections).Load(currentConnection)
				if !found {
					(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
					(*opt).requestNewServerIDChan <- true
					currentConnection = <-(*opt).getNewServerIDChan
					clearToSend = true
				} else {
					var raftConn = conn.(raft.RaftConnection)
					actionCall := raftConn.Connection.Go("RaftListener.ActionRPC", &actionArgs, &actionResponse, nil)
					go handleActionResponse(actionCall, &actionResponse, changeConnectionChan, msg, timestamp, currentConnection, opt, actionDoneChannel)
				}
			}
		}
	}
}

func handlePrematureTermination(termChan chan os.Signal, connectedChan chan bool) {
	select {
	case <-termChan:
		log.Info("Main - Shutting down before full connection...")
		os.Exit(0)
	case <-connectedChan:
	}
}

func main() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	log.SetLevel(log.InfoLevel)
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000000"
	log.SetFormatter(customFormatter)

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)
	connectedChan := make(chan bool)
	// Command line arguments
	args := os.Args
	if len(args) < 4 {
		log.Fatal("Usage: go_wanderer <Game|Test> <Client|Node|Bot> <Append|Full> <Port> ...<other raft ports>")
	}

	mode := args[1]
	nodeMode := args[2]
	connectionMode := args[3]
	port := args[4]

	var logOutputFile *os.File

	if mode == "Test" {
		logOutputFile, _ = os.OpenFile("/tmp/go_raft_log_"+port, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		log.SetOutput(logOutputFile)
	}
	go handlePrematureTermination(termChan, connectedChan)

	var playerID = engine.PlayerID(port)
	var serverID = raft.ServerID(port)
	var firstLeader = raft.ServerID("")
	// Get other servers
	otherServers := make([]raft.ServerID, 0)
	for i := 5; i < len(args); i++ {
		otherServers = append(otherServers, raft.ServerID(args[i]))
	}

	if len(otherServers) > 0 {
		firstLeader = otherServers[0]
	} else {
		firstLeader = serverID
	}

	// Client nodeMode: UI + Engine + Raft node
	var mainConnectedChan = make(chan bool)
	var mainDisconnectedChan = make(chan bool)
	var nodeConnectedChan = make(chan bool)
	var uiActionChan = make(chan engine.GameLog)
	var uiConfChan = make(chan bool)
	var stateReqChan chan bool
	var stateChan chan engine.GameState
	var actionChan chan raft.GameLog
	var snapshotRequestChan = make(chan bool)
	var snapshotResponseChan = make(chan []byte)
	var snapshotInstallChan = make(chan []byte)
	var currentTurnEngineChan = make(chan int)
	var currentTurnUIChan = make(chan int)
	var requestConnectionChan = make(chan raft.RequestConnection)
	var requestNewServerIDChan = make(chan bool)
	var getNewServerIDChan = make(chan raft.ServerID)
	var removedFromGameChan = make(chan bool)

	stateReqChan, stateChan, actionChan = engine.Start(playerID, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan, currentTurnEngineChan)

	var _ = raft.Start(port, otherServers, actionChan, nodeConnectedChan, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan)
	var nodeConnections = raft.ConnectToRaftServers(nil, raft.ServerID(port), otherServers)

	var opt = options{
		uiActionChan,
		uiConfChan,
		firstLeader,
		nodeConnections,
		otherServers,
		serverID,
		mainConnectedChan,
		mainDisconnectedChan,
		mode,
		requestConnectionChan,
		requestNewServerIDChan,
		getNewServerIDChan,
		removedFromGameChan}
	go connectionPool(&opt)
	go raft.ConnectionManager(nil, requestConnectionChan)
	go manageActions(&opt)
	go handleCurrentTurn(currentTurnEngineChan, currentTurnUIChan)

	if len(otherServers) > 0 {
		if connectionMode == "Append" {
			uiConfChan <- true
			// Wait for the node to be fully connected
			<-mainConnectedChan
		}
		// Notify the raft node
		nodeConnectedChan <- true
	} else {
		nodeConnectedChan <- true
	}

	if nodeMode == "Client" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan, currentTurnUIChan, false)
	} else if nodeMode == "Bot" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan, currentTurnUIChan, true)
	}

	connectedChan <- true
	<-termChan
	log.Info("Shutting down")
	uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), "Disconnect", engine.ActionImpl{engine.DISCONNECT, 0}}
	<-removedFromGameChan
	uiConfChan <- false
	select {
	case <-mainDisconnectedChan:
		log.Info("Shutdown complete (clean)")
	case <-time.After(time.Millisecond * 5000):
		log.Info("Shutdown complete (timeout)")
	}
}
