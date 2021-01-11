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
