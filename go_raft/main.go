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
	"time"
)

const actionCallTimeout = 500

/* TODO:
Two modes of initialization:
	* Node
		* Knows from the beginning to whom it should talk to
	* UI + Engine + Node
		* Knows at least one address
		* Gets all other nodes from this one
*/

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

// Setup UDP server to get commands from UI
// Setup a channel to get

func checkError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

// Returns an arbitrary connection id from the map
func getOneConnectionID(connections *map[raft.ServerID]*rpc.Client) raft.ServerID {
	for id := range *connections {
		return id
	}
	return ""
}

func handleActionResponse(call *rpc.Call, response *raft.ActionResponse, newConnectionChan chan raft.ServerID) {
	select {
	case <-call.Done:
		if !(*response).Applied {
			newConnectionChan <- (*response).LeaderID
		}
	case <-time.After(time.Millisecond * actionCallTimeout):
		fmt.Println("ActionRPC: Did not receive response from node")
		// TODO: handle error
	}
}

func manageActions(actionChan chan engine.GameLog, connections *map[raft.ServerID]*rpc.Client) {
	var currentConnection = getOneConnectionID(connections)
	newConnectionID := make(chan raft.ServerID)
	for {
		select {
		case msg := <-actionChan:
			var actionResponse raft.ActionResponse
			var actionArgs = raft.ActionArgs{msg.Id, msg.Action}
			actionCall := (*connections)[currentConnection].Go("RaftListener.ActionRPC", &actionArgs, &actionResponse, nil)
			go handleActionResponse(actionCall, &actionResponse, newConnectionID)
		case newLeaderID := <-newConnectionID:
			currentConnection = newLeaderID
		}
	}
}

func addSelfConnection(port string, connections *map[raft.ServerID]*rpc.Client) {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:"+string(port))
	checkError(err)
	(*connections)[raft.ServerID(port)] = client
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
		var nodeConnections = raft.Start(mode, port, otherServers, actionChan)
		addSelfConnection(port, nodeConnections)
		manageActions(uiActionChan, nodeConnections)
	} else {
		raft.Start(mode, port, otherServers, nil)
		for {
			select {
			case <-time.After(time.Second * 5):
			}
		}
	}

	/* TODO:
	if mode === Client
	start engine ->
		get a channel for communications node -> engine
		get a channel for communications ui <-> engine
	start raft node ->
		get a channel to communicate messages
	start ui ->
		the ui handles a udp client to send commands to the leader
	start udp server ->
		communicate received commands to the node through the channel
	so in order start: engine, raft node, udp server, ui
	*/
	// TODO: msgChan should be handled by UDP server

}
