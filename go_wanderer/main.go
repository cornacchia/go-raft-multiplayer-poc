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
	connections            *sync.Map
	otherServers           []raft.ServerID
	id                     raft.ServerID
	connectedChan          chan bool
	disconnectedChan       chan bool
	mode                   string
	nodeMode               string
	requestConnectionChan  chan raft.RequestConnection
	requestNewServerIDChan chan bool
	getNewServerIDChan     chan raft.ServerID
	uiStateChan            chan []byte
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

func handleActionResponse(call *rpc.Call, response *raft.ActionResponse, changeConnectionChan chan raft.ServerID, msg engine.GameLog, timestamp int64, currentConnection raft.ServerID, opt *options) {
	var waitTime time.Duration = 1000
	if msg.Action.Action == engine.CONNECT {
		waitTime = 5000
	}
	if call == nil {
		time.Sleep(time.Millisecond * 300)
		(*opt).actionChan <- msg
		return
	}

	select {
	case <-call.Done:
		if !(*response).Applied {
			log.Trace("Main - Action not applied ", currentConnection, " - ", (*response))
			if (*response).LeaderID != "" {
				addToKnownServers(opt, (*response).LeaderID)
				changeConnectionChan <- (*response).LeaderID
			} else {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, [2]bool{false, true}, (*opt).connections}
				changeConnectionChan <- (*opt).id
			}
			time.Sleep(time.Millisecond * 300)
			// Send again
			(*opt).actionChan <- msg
		} else if msg.Action.Action == engine.CONNECT {
			(*opt).connectedChan <- true
		} else if msg.Action.Action == engine.DISCONNECT {
			(*opt).disconnectedChan <- true
		} else if msg.Type != "NOOP" && (*opt).mode == "Test" && (*opt).nodeMode != "Node" {
			var now = getNowMs()
			log.Info("Main - Action time: ", (now - timestamp))
		}
		if (*opt).nodeMode == "Client" || (*opt).nodeMode == "Bot" {
			(*opt).uiStateChan <- (*response).State
		}
	case <-time.After(time.Millisecond * waitTime):
		if msg.Action.Action == engine.CONNECT {
			log.Debug("Main - Timeout connecting to raft network - ", currentConnection, " - ", msg)
		} else if msg.Action.Action != engine.DISCONNECT && (*opt).mode == "Test" {
			log.Info("Main - Action dropped - ", currentConnection, " - ", msg)
		}
		changeConnectionChan <- ""
		(*opt).actionChan <- msg
	}
}

func manageActions(opt *options) {
	(*opt).requestNewServerIDChan <- true
	currentConnection := <-(*opt).getNewServerIDChan
	changeConnectionChan := make(chan raft.ServerID)
	for {
		select {
		case msg := <-(*opt).actionChan:
			var timestamp = getNowMs()
			var actionResponse raft.ActionResponse
			var jsonAction, _ = json.Marshal(msg.Action)
			var actionArgs = raft.ActionArgs{string(msg.Id), msg.ActionId, msg.Type, jsonAction}
			var conn, found = (*(*opt).connections).Load(currentConnection)
			if !found {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, [2]bool{false, true}, (*opt).connections}
				go handleActionResponse(nil, nil, changeConnectionChan, msg, timestamp, currentConnection, opt)
				(*opt).requestNewServerIDChan <- true
				currentConnection = <-(*opt).getNewServerIDChan
			} else {
				var raftConn = conn.(raft.RaftConnection)
				actionCall := raftConn.Connection.Go("RaftListener.ActionRPC", &actionArgs, &actionResponse, nil)
				go handleActionResponse(actionCall, &actionResponse, changeConnectionChan, msg, timestamp, currentConnection, opt)
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
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, [2]bool{false, true}, (*opt).connections}
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
		log.Fatal("Usage: go_wanderer <Game|Test> <Client|Node|Bot> <Port> ...<other raft ports>")
	}

	mode := args[1]
	nodeMode := args[2]
	port := args[3]

	var logOutputFile *os.File

	if mode == "Test" {
		logOutputFile, _ = os.OpenFile("/tmp/go_raft_log_"+port, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		log.SetOutput(logOutputFile)
	}

	go handlePrematureTermination(termChan, connectedChan)

	var playerID = engine.PlayerID(port)
	var serverID = raft.ServerID(port)
	// Get other servers
	otherServers := make([]raft.ServerID, 0)
	for i := 4; i < len(args); i++ {
		otherServers = append(otherServers, raft.ServerID(args[i]))
	}

	// Client nodeMode: UI + Engine + Raft node
	var mainConnectedChan = make(chan bool)
	var mainDisconnectedChan = make(chan bool)
	var nodeConnectedChan = make(chan bool)
	var uiActionChan = make(chan engine.GameLog)
	var stateChan chan []byte
	var uiStateChan = make(chan []byte)
	var actionChan chan raft.GameLog
	var snapshotRequestChan = make(chan bool)
	var snapshotResponseChan = make(chan []byte)
	var snapshotInstallChan = make(chan []byte)
	var currentTurnEngineChan = make(chan int)
	var currentTurnUIChan = make(chan int)
	var requestConnectionChan = make(chan raft.RequestConnection)
	var requestNewServerIDChan = make(chan bool)
	var getNewServerIDChan = make(chan raft.ServerID)

	if nodeMode == "Node" {
		stateChan, actionChan = engine.Start(playerID, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan)
		var _ = raft.Start(nodeMode, port, otherServers, actionChan, stateChan, nodeConnectedChan, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan)
	}
	var nodeConnections, _ = raft.ConnectToRaftServers(nil, raft.ServerID(port), otherServers)

	var opt = options{
		uiActionChan,
		nodeConnections,
		otherServers,
		serverID,
		mainConnectedChan,
		mainDisconnectedChan,
		mode,
		nodeMode,
		requestConnectionChan,
		requestNewServerIDChan,
		getNewServerIDChan,
		uiStateChan}
	go connectionPool(&opt)
	go raft.ConnectionManager(nil, requestConnectionChan)
	go manageActions(&opt)
	go handleCurrentTurn(currentTurnEngineChan, currentTurnUIChan)

	if nodeMode == "Node" && len(otherServers) > 0 {
		uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), "Connect", engine.ActionImpl{engine.CONNECT, 0}}
		// Wait for the node to be fully connected
		<-mainConnectedChan
		// Notify the raft node
		nodeConnectedChan <- true
	}

	if nodeMode == "Client" {
		ui.Start(playerID, uiActionChan, uiStateChan, currentTurnUIChan, currentTurnEngineChan, false)
	} else if nodeMode == "Bot" {
		ui.Start(playerID, uiActionChan, uiStateChan, currentTurnUIChan, currentTurnEngineChan, true)
	}

	connectedChan <- true
	<-termChan
	log.Info("Shutting down")
	if nodeMode != "Node" {
		select {
		case <-mainDisconnectedChan:
			log.Info("Shutdown complete (clean)")
		case <-time.After(time.Millisecond * 5000):
			log.Info("Shutdown complete (timeout)")
		}
	} else {
		uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), "Disconnect", engine.ActionImpl{engine.DISCONNECT, 0}}
		select {
		case <-mainDisconnectedChan:
			log.Info("Shutdown complete (clean)")
		case <-time.After(time.Millisecond * 5000):
			log.Info("Shutdown complete (timeout)")
		}
	}
}
