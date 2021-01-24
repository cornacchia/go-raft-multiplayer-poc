package main

import (
	"go_raft/engine"
	"go_raft/raft"
	"go_raft/ui"
	"math/rand"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

const actionCallTimeout = 500

/*
Configuration change (preparation):
	* The node joins as a non voting member
	* Logs are replicated but it is not taken in consideration for votes
	* Once it catches up the second phase starts
Configuration change:
	* The leader receives notice of a new node (e.g. new client)
	* It stores the configuration for joint consensus Cold,new as a log entry
	* Once a follower receives that entry it uses it for new decisions
	* Once Cold,new has been committed the server can create an entry with Cnew
If the leader is not part of the next configuration:
	* It steps down after having committed the relative log
*/

func checkError(err error) {
	if err != nil {
		log.Error("Error: ", err)
	}
}

// Returns an arbitrary connection id from the map
func getOneConnectionID(connections *sync.Map, otherServers []raft.ServerID, myID raft.ServerID) raft.ServerID {
	var returnID raft.ServerID = ""
	(*connections).Range(func(id interface{}, connection interface{}) bool {
		if len(otherServers) > 0 {
			if id.(raft.ServerID) != myID {
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

func handleActionResponse(call *rpc.Call, response *raft.ActionResponse, newConnectionChan chan raft.ServerID, actionChan chan engine.GameLog, msg engine.GameLog, connectedChan chan bool, disconnectedChan chan bool) {
	select {
	case <-call.Done:
		if !(*response).Applied {
			if (*response).LeaderID != "" {
				newConnectionChan <- (*response).LeaderID
			}
			time.Sleep(time.Millisecond * 500)
			// Send again
			actionChan <- msg
		} else if msg.Action == engine.CONNECT {
			connectedChan <- true
		} else if msg.Action == engine.DISCONNECT {
			disconnectedChan <- true
		}
	case <-time.After(time.Millisecond * actionCallTimeout):
		// Send again
		actionChan <- msg
	}
}

func manageActions(actionChan chan engine.GameLog, connections *sync.Map, otherServers []raft.ServerID, id raft.ServerID, connectedChan chan bool, disconnectedChan chan bool) {
	var currentConnection = getOneConnectionID(connections, otherServers, id)
	newConnectionID := make(chan raft.ServerID)
	for {
		select {
		case msg := <-actionChan:
			var actionResponse raft.ActionResponse
			var actionArgs = raft.ActionArgs{msg.Id, msg.Action}
			var conn, _ = (*connections).Load(currentConnection)
			var raftConn = conn.(raft.RaftConnection)
			actionCall := raftConn.Connection.Go("RaftListener.ActionRPC", &actionArgs, &actionResponse, nil)
			go handleActionResponse(actionCall, &actionResponse, newConnectionID, actionChan, msg, connectedChan, disconnectedChan)
		case newLeaderID := <-newConnectionID:
			currentConnection = newLeaderID
		}
	}
}

func addSelfConnection(port string, connections *sync.Map) {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:"+string(port))
	checkError(err)
	(*connections).Store(raft.ServerID(port), client)
}

func main() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())
	log.SetLevel(log.DebugLevel)

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)
	// Command line arguments
	args := os.Args
	if len(args) < 3 {
		log.Fatal("No port provided!")
	}
	mode := args[1]
	port := args[2]

	intPort, err := strconv.Atoi(port)
	checkError(err)
	var playerID = engine.PlayerID(intPort)
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
	var actionChan chan engine.GameLog

	if mode == "Client" {
		stateReqChan, stateChan, actionChan = engine.Start(playerID)
	}

	var _ = raft.Start(mode, port, otherServers, actionChan, nodeConnectedChan)
	var nodeConnections, _ = raft.ConnectToRaftServers(nil, raft.ServerID(port), otherServers)

	go manageActions(uiActionChan, nodeConnections, otherServers, serverID, mainConnectedChan, mainDisconnectedChan)

	if len(otherServers) > 0 {
		uiActionChan <- engine.GameLog{playerID, engine.CONNECT, nil}
		// Wait for the node to be fully connected
		<-mainConnectedChan
		// Notify the raft node
		nodeConnectedChan <- true
	}

	if mode == "Client" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan)
	}

	<-termChan
	log.Info("Shutting down...")
	uiActionChan <- engine.GameLog{playerID, engine.DISCONNECT, nil}
	select {
	case <-mainDisconnectedChan:
	case <-time.After(time.Millisecond * 5000):
	}
}
