package main

import (
	"fmt"
	"go_raft/engine"
	"go_raft/raft"
	"go_raft/ui"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
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
		fmt.Println("Error: ", err)
	}
}

// Returns an arbitrary connection id from the map
func getOneConnectionID(connections *sync.Map) raft.ServerID {
	var returnID raft.ServerID = ""
	(*connections).Range(func(id interface{}, connection interface{}) bool {
		returnID = id.(raft.ServerID)
		return false
	})

	return returnID
}

func handleActionResponse(call *rpc.Call, response *raft.ActionResponse, newConnectionChan chan raft.ServerID, actionChan chan engine.GameLog, msg engine.GameLog) {
	select {
	case <-call.Done:
		if !(*response).Applied {
			if (*response).LeaderID != "" {
				newConnectionChan <- (*response).LeaderID
			}
			// Send again
			actionChan <- msg
		}
	case <-time.After(time.Millisecond * actionCallTimeout):
		// Send again
		actionChan <- msg
	}
}

func manageActions(actionChan chan engine.GameLog, connections *sync.Map) {
	var currentConnection = getOneConnectionID(connections)
	newConnectionID := make(chan raft.ServerID)
	for {
		select {
		case msg := <-actionChan:
			var actionResponse raft.ActionResponse
			var actionArgs = raft.ActionArgs{msg.Id, msg.Action}
			var conn, _ = (*connections).Load(currentConnection)
			actionCall := (*conn.(*rpc.Client)).Go("RaftListener.ActionRPC", &actionArgs, &actionResponse, nil)
			go handleActionResponse(actionCall, &actionResponse, newConnectionID, actionChan, msg)
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

func createConnections(conn *sync.Map) *sync.Map {
	var newConnections sync.Map
	(*conn).Range(func(id interface{}, connection interface{}) bool {
		newConnections.Store(id.(raft.ServerID), connection.(*rpc.Client))
		return true
	})
	return &newConnections
}

func main() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Command line arguments
	args := os.Args
	if len(args) < 3 {
		fmt.Println("No port provided!")
		os.Exit(0)
	}
	mode := args[1]
	port := args[2]

	intPort, err := strconv.Atoi(port)
	checkError(err)
	var playerID = engine.PlayerID(intPort)

	// Get other servers
	otherServers := make([]raft.ServerID, 0)
	for i := 3; i < len(args); i++ {
		otherServers = append(otherServers, raft.ServerID(args[i]))
	}

	if mode == "Client" {
		// Client mode: UI + Engine + Raft node
		var uiActionChan = make(chan engine.GameLog)
		var stateReqChan, stateChan, actionChan = engine.Start(playerID)
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan)
		var _ = raft.Start(mode, port, otherServers, actionChan)
		var nodeConnections, _ = raft.ConnectToRaftServers(raft.ServerID(port), otherServers)
		//var nodeConnections = createConnections(conn)
		//addSelfConnection(port, nodeConnections)
		manageActions(uiActionChan, nodeConnections)
	} else {
		raft.Start(mode, port, otherServers, nil)
		for {
			select {
			case <-time.After(time.Second * 5):
			}
		}
	}
}
