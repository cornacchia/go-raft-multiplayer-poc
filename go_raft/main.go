package main

import (
	"fmt"
	"go_raft/engine"
	"go_raft/raft"
	"go_raft/ui"
	"math/rand"
	"os"
	"strconv"
	"time"
)

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
		var stateReqChan, stateChan, actionChan = engine.Start(playerID)
		ui.Start(playerID, stateReqChan, stateChan, otherServers)
		var msgChan = raft.Start(mode, port, otherServers, actionChan)
		// Start UDP server to receive messages from UIs and send them to Raft through msgChan
	} else {
		raft.Start(port, otherServers)
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
