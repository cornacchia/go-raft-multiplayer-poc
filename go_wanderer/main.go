package main

import (
	"encoding/json"
	"fmt"
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

const actionCallTimeout = 300

type options struct {
	actionChan       chan engine.GameLog
	connections      *sync.Map
	otherServers     []raft.ServerID
	id               raft.ServerID
	connectedChan    chan bool
	disconnectedChan chan bool
	outputFile       *os.File
	mode             string
}

func checkError(err error) {
	if err != nil {
		log.Error("Error: ", err)
	}
}

func getNowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// Returns an arbitrary connection id from the map
func getOneConnectionID(opt *options) raft.ServerID {
	var returnID raft.ServerID = ""
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		if len((*opt).otherServers) > 0 {
			if id.(raft.ServerID) != (*opt).id {
				returnID = id.(raft.ServerID)
				return false
			}
			return true
		}
		returnID = id.(raft.ServerID)
		return false
	})

	return returnID
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

func handleActionResponse(call *rpc.Call, response *raft.ActionResponse, newConnectionChan chan raft.ServerID, msg engine.GameLog, timestamp int64, opt *options) {
	select {
	case <-call.Done:
		if !(*response).Applied {
			if (*response).LeaderID != "" {
				newConnectionChan <- (*response).LeaderID
			} else {
				newConnectionChan <- (*opt).id
			}
			time.Sleep(time.Millisecond * 500)
			// TODO se non risponde entro un tot cambia leader ID
			// Send again
			(*opt).actionChan <- msg
		} else if msg.Action.Action == engine.CONNECT {
			(*opt).connectedChan <- true
		} else if msg.Action.Action == engine.DISCONNECT {
			(*opt).disconnectedChan <- true
		} else if (*opt).mode == "Test" {
			var now = getNowMs()
			(*opt).outputFile.Write([]byte(fmt.Sprintf("%v\n", (now - timestamp))))
		}
	case <-time.After(time.Millisecond * actionCallTimeout):
		if msg.Action.Action == engine.CONNECT {
			log.Debug("Timeout connecting to raft network")
			(*opt).actionChan <- msg
		} else if msg.Action.Action != engine.DISCONNECT && (*opt).mode == "Test" {
			(*opt).outputFile.Write([]byte("Action dropped"))
		}
		// Send again
		newConnectionChan <- (*opt).id
		// TODO proviamo a non rimandare per vedere se evita memory leak
		(*opt).actionChan <- msg
	}
}

func manageActions(opt *options) {
	var currentConnection = getOneConnectionID(opt)
	newConnectionID := make(chan raft.ServerID)
	for {
		select {
		case msg := <-(*opt).actionChan:
			log.Trace("Sending action for turn: ", msg.Action.Turn)
			var timestamp = getNowMs()
			var actionResponse raft.ActionResponse
			var jsonAction, _ = json.Marshal(msg.Action)
			var actionArgs = raft.ActionArgs{string(msg.Id), msg.ActionId, msg.Type, jsonAction}
			var conn, _ = (*(*opt).connections).Load(currentConnection)
			var raftConn = conn.(raft.RaftConnection)
			actionCall := raftConn.Connection.Go("RaftListener.ActionRPC", &actionArgs, &actionResponse, nil)
			go handleActionResponse(actionCall, &actionResponse, newConnectionID, msg, timestamp, opt)
		case newLeaderID := <-newConnectionID:
			currentConnection = newLeaderID
			var _, found = (*(*opt).connections).Load(currentConnection)
			if !found {
				log.Info("Need to connect to new leader: ", newLeaderID)
				resultChan := make(chan *raft.RaftConnectionResponse)
				go raft.ConnectToRaftServer(nil, currentConnection, resultChan)
				var resp = <-resultChan
				var newConnection = raft.RaftConnection{(*resp).Connection, false, true}
				(*(*opt).connections).Store((*resp).Id, newConnection)
			}
		}
	}
}

func handlePrematureTermination(termChan chan os.Signal, connectedChan chan bool) {
	select {
	case <-termChan:
		log.Info("Shutting down before full connection...")
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
		log.Fatal("Usage: go_skeletons <Game|Test> <Client|Node|Bot> <Port> ...<other raft ports>")
	}

	mode := args[1]
	nodeMode := args[2]
	port := args[3]

	var outputFile *os.File
	var logOutputFile *os.File

	if mode == "Test" {
		outputFile, _ = os.OpenFile("/tmp/go_raft_"+port, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

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
	var stateReqChan chan bool
	var stateChan chan engine.GameState
	var actionChan chan raft.GameLog
	var snapshotRequestChan = make(chan bool)
	var snapshotResponseChan = make(chan []byte)
	var snapshotInstallChan = make(chan []byte)
	var currentTurnEngineChan = make(chan int)
	var currentTurnUIChan = make(chan int)

	stateReqChan, stateChan, actionChan = engine.Start(playerID, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan, currentTurnEngineChan)

	var _ = raft.Start(nodeMode, port, otherServers, actionChan, nodeConnectedChan, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan)
	var nodeConnections, _ = raft.ConnectToRaftServers(nil, raft.ServerID(port), otherServers)

	var opt = options{
		uiActionChan,
		nodeConnections,
		otherServers,
		serverID,
		mainConnectedChan,
		mainDisconnectedChan,
		outputFile,
		mode}
	go manageActions(&opt)
	go handleCurrentTurn(currentTurnEngineChan, currentTurnUIChan)

	if len(otherServers) > 0 {
		uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), "Connect", engine.ActionImpl{engine.CONNECT, 0}}
		// Wait for the node to be fully connected
		<-mainConnectedChan
		// Notify the raft node
		nodeConnectedChan <- true
	}

	if nodeMode == "Client" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan, currentTurnUIChan, false)
	} else if nodeMode == "Bot" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan, currentTurnUIChan, true)
	}

	connectedChan <- true

	<-termChan
	log.Info("Shutting down...")
	uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), "Disconnect", engine.ActionImpl{engine.DISCONNECT, 0}}

	select {
	case <-mainDisconnectedChan:
	case <-time.After(time.Millisecond * 5000):
	}
}
