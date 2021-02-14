package main

import (
	"crypto"
	rng "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"go_raft/raft"
	"go_skeletons/engine"
	"go_skeletons/ui"
	"io/ioutil"
	"math/rand"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
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
	requestConnectionChan  chan raft.RequestConnection
	requestNewServerIDChan chan bool
	getNewServerIDChan     chan raft.ServerID
	clientPrivateKey       *rsa.PrivateKey
}

func checkError(err error) {
	if err != nil {
		log.Error("Error: ", err)
	}
}

func getActionArgsSignature(privKey *rsa.PrivateKey, aa *raft.ActionArgs) []byte {
	hashed := raft.GetActionArgsBytes(aa)
	signature, err := rsa.SignPKCS1v15(rng.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
}

func getUpdateLeaderSignature(privKey *rsa.PrivateKey, ua *raft.UpdateLeaderArgs) []byte {
	hashed := raft.GetUpdateLeaderArgsBytes(ua)
	signature, err := rsa.SignPKCS1v15(rng.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
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

func broadcastUpdateLeaderRPC(opt *options, updateLeaderArgs *raft.UpdateLeaderArgs, changeConnectionChan chan raft.ServerID) {
	(*(*opt).connections).Range(func(id interface{}, connection interface{}) bool {
		if id.(raft.ServerID) == (*opt).id {
			return true
		}

		var raftConn = connection.(raft.RaftConnection)
		var response raft.UpdateLeaderResponse
		actionCall := raftConn.Connection.Go("RaftListener.UpdateLeaderRPC", updateLeaderArgs, &response, nil)
		go func(opt *options, actionCall *rpc.Call, id raft.ServerID) {
			select {
			case <-actionCall.Done:
				if !response.Success && response.LeaderID != "" {
					changeConnectionChan <- response.LeaderID
				}
			case <-time.After(time.Millisecond * 500):
				log.Warning("UpdateLeaderRPC: Did not receive response from: " + string(id))
				(*opt).requestConnectionChan <- raft.RequestConnection{id, [2]bool{false, true}, (*opt).connections}
			}
		}(opt, actionCall, id.(raft.ServerID))

		return true
	})
}

func handleActionResponse(call *rpc.Call, response *raft.ActionResponse, changeConnectionChan chan raft.ServerID, msg engine.GameLog, timestamp int64, currentConnection raft.ServerID, opt *options) {
	var waitTime time.Duration = 5000
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
		} else if (*opt).mode == "Test" {
			var now = getNowMs()
			log.Info("Main - Action time: ", (now - timestamp))
		}
	case <-time.After(time.Millisecond * waitTime):
		if msg.Action.Action == engine.CONNECT {
			log.Debug("Main - Timeout connecting to raft network - ", currentConnection, " - ", msg)
		} else if msg.Action.Action != engine.DISCONNECT && (*opt).mode == "Test" {
			log.Info("Main - Action dropped - ", currentConnection, " - ", msg)
		}
		var updateLeaderArgs = raft.UpdateLeaderArgs{string(msg.Id), raft.ServerID(currentConnection), []byte{}}
		updateLeaderArgs.Signature = getUpdateLeaderSignature((*opt).clientPrivateKey, &updateLeaderArgs)
		broadcastUpdateLeaderRPC(opt, &updateLeaderArgs, changeConnectionChan)
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
			var actionArgs = raft.ActionArgs{string(msg.Id), msg.ActionId, msg.Type, jsonAction, []byte{}}
			actionArgs.Signature = getActionArgsSignature((*opt).clientPrivateKey, &actionArgs)
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

func readRSAPublicKey(fileName string) *rsa.PublicKey {
	fileBytes, err := ioutil.ReadFile(fileName)
	checkError(err)
	block, _ := pem.Decode(fileBytes)
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	checkError(err)
	return key.(*rsa.PublicKey)
}

func readRSAPrivateKey(fileName string) *rsa.PrivateKey {
	fileBytes, err := ioutil.ReadFile(fileName)
	checkError(err)
	block, _ := pem.Decode(fileBytes)
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	checkError(err)
	return key
}

// Each BFT Raft node needs: public keys for all servers, public keys for all clients, own private keys
func readRSAKeys(id string) (map[raft.ServerID]*rsa.PublicKey, map[string]*rsa.PublicKey, *rsa.PrivateKey, *rsa.PrivateKey) {
	nodeKeys := make(map[raft.ServerID]*rsa.PublicKey)
	clientKeys := make(map[string]*rsa.PublicKey)
	var privateNodeKey = readRSAPrivateKey("../keys/nodes/key_" + id + ".pem")
	var privateClientKey = readRSAPrivateKey("../keys/clients/key_" + id + ".pem")

	nodeFiles, err := ioutil.ReadDir("../keys/nodes")
	checkError(err)
	for _, f := range nodeFiles {
		if strings.Contains(f.Name(), "public") {
			var nodeID = raft.ServerID(strings.Split(strings.Split(f.Name(), ".")[0], "_")[2])
			nodeKeys[nodeID] = readRSAPublicKey("../keys/nodes/" + f.Name())
		}
	}

	clientFiles, err := ioutil.ReadDir("../keys/clients")
	checkError(err)
	for _, f := range clientFiles {
		if strings.Contains(f.Name(), "public") {
			var clientID = strings.Split(strings.Split(f.Name(), ".")[0], "_")[2]
			clientKeys[clientID] = readRSAPublicKey("../keys/clients/" + f.Name())
		}
	}

	return nodeKeys, clientKeys, privateNodeKey, privateClientKey
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

	nodeKeys, clientKeys, nodePrivateKey, clientPrivateKey := readRSAKeys(port)

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
	var stateReqChan chan bool
	var stateChan chan engine.GameState
	var actionChan chan raft.GameLog
	var snapshotRequestChan = make(chan bool)
	var snapshotResponseChan = make(chan []byte)
	var snapshotInstallChan = make(chan []byte)
	var requestConnectionChan = make(chan raft.RequestConnection)
	var requestNewServerIDChan = make(chan bool)
	var getNewServerIDChan = make(chan raft.ServerID)

	stateReqChan, stateChan, actionChan = engine.Start(playerID, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan)

	var _ = raft.Start(nodeMode, port, otherServers, actionChan, nodeConnectedChan, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan, nodeKeys, clientKeys, nodePrivateKey)
	var nodeConnections, _ = raft.ConnectToRaftServers(nil, raft.ServerID(port), otherServers)

	var opt = options{
		uiActionChan,
		nodeConnections,
		otherServers,
		serverID,
		mainConnectedChan,
		mainDisconnectedChan,
		mode,
		requestConnectionChan,
		requestNewServerIDChan,
		getNewServerIDChan,
		clientPrivateKey}
	go connectionPool(&opt)
	go raft.ConnectionManager(nil, requestConnectionChan)
	go manageActions(&opt)

	if len(otherServers) > 0 {
		uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), "Connect", engine.ActionImpl{engine.CONNECT}}
		// Wait for the node to be fully connected
		<-mainConnectedChan
		// Notify the raft node
		nodeConnectedChan <- true
	}

	if nodeMode == "Client" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan, false)
	} else if nodeMode == "Bot" {
		ui.Start(playerID, stateReqChan, stateChan, uiActionChan, true)
	}

	connectedChan <- true
	<-termChan
	log.Info("Main - Shutting down")
	uiActionChan <- engine.GameLog{playerID, ui.GetActionID(), "Disconnect", engine.ActionImpl{engine.DISCONNECT}}
	select {
	case <-mainDisconnectedChan:
		log.Info("Main - Shutdown complete (clean)")
	case <-time.After(time.Millisecond * 5000):
		log.Info("Main - Shutdown complete (timeout)")
	}
}
