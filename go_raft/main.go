package main

import (
	"fmt"
	"go_raft/engine"
	"go_raft/raft"
	"go_raft/ui"
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
		} else if msg.Action == engine.CONNECT {
			(*opt).connectedChan <- true
		} else if msg.Action == engine.DISCONNECT {
			(*opt).disconnectedChan <- true
		} else {
			var now = getNowMs()
			(*opt).outputFile.Write([]byte(fmt.Sprintf("%v\n", (now - timestamp))))
		}
	case <-time.After(time.Millisecond * actionCallTimeout):
		if msg.Action == engine.CONNECT {
			log.Debug("Timeout connecting to raft network")
			(*opt).actionChan <- msg
		} else if msg.Action != engine.DISCONNECT {
			(*opt).outputFile.Write([]byte("Action dropped"))
		}
		// Send again
		newConnectionChan <- (*opt).id
		// TODO proviamo a non rimandare per vedere se evita memory leak
		//(*opt).actionChan <- msg
	}
}

func manageActions(opt *options) {
	var currentConnection = getOneConnectionID(opt)
	newConnectionID := make(chan raft.ServerID)
	for {
		select {
		case msg := <-(*opt).actionChan:
			var timestamp = getNowMs()
			var actionResponse raft.ActionResponse
			var actionArgs = raft.ActionArgs{string(msg.Id), msg.ActionId, msg.Action}
			var conn, _ = (*(*opt).connections).Load(currentConnection)
			var raftConn = conn.(raft.RaftConnection)
			actionCall := raftConn.Connection.Go("RaftListener.ActionRPC", &actionArgs, &actionResponse, nil)
			go handleActionResponse(actionCall, &actionResponse, newConnectionID, msg, timestamp, opt)
		case newLeaderID := <-newConnectionID:
			log.Trace("Main: New leader id ", newLeaderID)
			currentConnection = newLeaderID
			var _, found = (*(*opt).connections).Load(currentConnection)
			if !found {
				resultChan := make(chan *raft.RaftConnectionResponse)
				go raft.ConnectToRaftServer(nil, currentConnection, resultChan)
				var resp = <-resultChan
				var newConnection = raft.RaftConnection{(*resp).Connection, false, true}
				(*(*opt).connections).Store((*resp).Id, newConnection)
			}
		}
	}
}

func handlePrematureTermination(termChan chan os.Signal, connectedChan chan bool, outputFile *os.File, logOutputFile *os.File) {
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

	log.SetLevel(log.DebugLevel)

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)
	connectedChan := make(chan bool)
	// Command line arguments
	args := os.Args
	if len(args) < 3 {
		log.Fatal("No port provided!")
	}
	mode := args[1]
	port := args[2]

	outputFile, err := os.OpenFile("/tmp/go_raft_"+port, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	checkError(err)

	logOutputFile, err := os.OpenFile("/tmp/go_raft_log_"+port, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	checkError(err)

	go handlePrematureTermination(termChan, connectedChan, outputFile, logOutputFile)
	// log.SetOutput(logOutputFile)
	checkError(err)
	var playerID = engine.PlayerID(port)
	var serverID = raft.ServerID(port)
	// Get other servers
	otherServers := make([]raft.ServerID, 0)
	for i := 3; i < len(args); i++ {
		otherServers = append(otherServers, raft.ServerID(args[i]))
	}

	// Client mode: UI + Engine + Raft node
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

	stateReqChan, stateChan, actionChan = engine.Start(playerID, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan)

	var _ = raft.Start(mode, port, otherServers, actionChan, nodeConnectedChan, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan)
	var nodeConnections, _ = raft.ConnectToRaftServers(nil, raft.ServerID(port), otherServers)

	var opt = options{
		uiActionChan,
		nodeConnections,
		otherServers,
		serverID,
		mainConnectedChan,
		mainDisconnectedChan,
		outputFile}
	go manageActions(&opt)

	if len(otherServers) > 0 {
		uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), engine.CONNECT}
		// Wait for the node to be fully connected
		<-mainConnectedChan
		// Notify the raft node
		nodeConnectedChan <- true
	}

	if mode == "Client" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan, false)
	} else if mode == "Bot" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan, true)
	}
	connectedChan <- true
	<-termChan
	log.Info("Shutting down...")
	uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), engine.DISCONNECT}
	select {
	case <-mainDisconnectedChan:
	case <-time.After(time.Millisecond * 2000):
	}
}
