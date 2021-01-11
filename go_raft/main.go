package main

import (
	"fmt"
	"go_raft/core"
	"math/rand"
	"os"
	"time"
)

func debugMsgs(msgChan chan core.GameLog) {
	for {
		var newLog = core.GameLog{1, 1}
		msgChan <- newLog
		time.Sleep(1 * time.Second)
	}
}

func prodMode() {
	for {
		time.Sleep(1 * time.Second)
	}
}

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

func main() {
	rand.Seed(time.Now().UnixNano())
	args := os.Args
	if len(args) < 3 {
		fmt.Println("No port provided!")
		os.Exit(0)
	}
	mode := args[1]
	port := args[2]
	otherServers := make([]core.ServerID, 0)
	for i := 3; i < len(args); i++ {
		otherServers = append(otherServers, core.ServerID(args[i]))
	}
	// TODO: msgChan should be handled by UDP server
	msgChan := core.Start(port, otherServers)
	if mode == "debug" {
		debugMsgs(msgChan)
	} else {
		prodMode()
	}
}
