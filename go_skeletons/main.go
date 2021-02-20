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
	configurationChan      chan bool
	firstLeader            raft.ServerID
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
	removedFromGameChan    chan bool
	actionQuorum           sync.Map
	allServers             []raft.ServerID
}

func checkError(err error) {
	if err != nil {
		log.Error("Error: ", err)
	}
}

func checkActionQuorum(opt *options, id int64) bool {
	var currentValue, found = (*opt).actionQuorum.Load(id)
	if !found {
		log.Trace("Action id not found: ", id)
		return false
	}
	log.Trace("Action quorum: ", currentValue.(int)+1, "/", len((*opt).allServers))
	if currentValue.(int)+1 > len((*opt).allServers)/2 {
		(*opt).actionQuorum.LoadAndDelete(id)
		return true
	}
	(*opt).actionQuorum.Store(id, currentValue.(int)+1)
	return false
}

func getActionArgsSignature(privKey *rsa.PrivateKey, aa *raft.ActionArgs) []byte {
	hashed := raft.GetActionArgsBytes(aa)
	signature, err := rsa.SignPKCS1v15(rng.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
}

func getConfigurationArgsSignature(privKey *rsa.PrivateKey, ca *raft.AddRemoveServerArgs) []byte {
	hashed := raft.GetAddRemoveServerArgsBytes(ca)
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
				(*opt).requestConnectionChan <- raft.RequestConnection{id, (*opt).connections}
			}
		}(opt, actionCall, id.(raft.ServerID))

		return true
	})
}

func broadcastAction(opt *options, actionArgs *raft.ActionArgs, timestamp int64, actionDoneChannel chan bool) {
	chanResponse := make(chan bool, len((*opt).allServers))
	for _, id := range (*opt).allServers {
		conn, found := (*opt).connections.Load(id)
		if !found {
			log.Debug("Main - Connection not found, will not broadcast action for now: ", id)
			(*opt).requestConnectionChan <- raft.RequestConnection{id, (*opt).connections}
			continue
		}
		var actionResponse raft.ActionResponse
		var raftConn = conn.(raft.RaftConnection)
		actionCall := raftConn.Connection.Go("RaftListener.ActionRPC", actionArgs, &actionResponse, nil)
		go func(opt *options, actionCall *rpc.Call, response *raft.ActionResponse, actionId int64, id raft.ServerID) {
			select {
			case <-actionCall.Done:
				if !(*response).Applied {
					log.Trace("Main - Action not applied ", id, " - ", (*response))
					chanResponse <- false
				} else {
					chanResponse <- true
				}
			case <-time.After(time.Millisecond * 500):
				chanResponse <- false
			}
		}(opt, actionCall, &actionResponse, (*actionArgs).ActionId, id)
	}

	var done = false
	var timer = time.NewTimer(time.Millisecond * 500)
	for !done {
		select {
		case resp := <-chanResponse:
			if resp && checkActionQuorum(opt, (*actionArgs).ActionId) {
				if (*opt).mode == "Test" {
					var now = getNowMs()
					log.Info("Main - Action time: ", (now - timestamp), " - ", (*actionArgs).ActionId)
				}
				actionDoneChannel <- true
				done = true
			}
		case <-timer.C:
			log.Trace("Main - Action broadcast timeout")
			actionDoneChannel <- false
			done = true
		}
	}
}

/*
func handleActionResponse(call *rpc.Call, response *raft.ActionResponse, changeConnectionChan chan raft.ServerID, msg engine.GameLog, timestamp int64, currentConnection raft.ServerID, opt *options, actionDoneChannel chan bool) {
	var waitTime time.Duration = 1000
	select {
	case <-call.Done:
		if !(*response).Applied {
			log.Trace("Main - Action not applied ", currentConnection, " - ", (*response))
			if (*response).LeaderID != "" {
				addToKnownServers(opt, (*response).LeaderID)
				changeConnectionChan <- (*response).LeaderID
			} else {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
				changeConnectionChan <- (*opt).id
			}
			actionDoneChannel <- false
		} else {
			if (*opt).mode == "Test" {
				var now = getNowMs()
				log.Info("Main - Action time: ", (now - timestamp), " - ", msg.ActionId)
			}
			if checkActionQuorum(opt, msg.ActionId) {
				actionDoneChannel <- true
			}
		}

	case <-time.After(time.Millisecond * waitTime):
		if (*opt).mode == "Test" {
			log.Info("Main - Action dropped - ", currentConnection, " - ", msg.ActionId)
		}
		// changeConnectionChan <- ""
		var updateLeaderArgs = raft.UpdateLeaderArgs{string(msg.Id), raft.ServerID(currentConnection), []byte{}}
		updateLeaderArgs.Signature = getUpdateLeaderSignature((*opt).clientPrivateKey, &updateLeaderArgs)
		broadcastUpdateLeaderRPC(opt, &updateLeaderArgs, changeConnectionChan)
	}
}
*/

func handleConfigurationResponse(call *rpc.Call, response *raft.AddRemoveServerResponse, changeConnectionChan chan raft.ServerID, msg bool, currentConnection raft.ServerID, opt *options) {
	var waitTime time.Duration = 5000
	if call == nil {
		time.Sleep(time.Millisecond * 300)
		(*opt).configurationChan <- msg
		return
	}
	select {
	case <-call.Done:
		if !(*response).Success {
			log.Trace("Main - Connection not applied ", currentConnection, " - ", (*response))
			if (*response).LeaderID != "" {
				addToKnownServers(opt, (*response).LeaderID)
				changeConnectionChan <- (*response).LeaderID
			} else {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
				changeConnectionChan <- (*opt).id
			}
			time.Sleep(time.Millisecond * 300)
			// Send again
			(*opt).configurationChan <- msg
		} else {
			if msg {
				(*opt).connectedChan <- true
			} else {
				(*opt).disconnectedChan <- true
			}
		}
	case <-time.After(time.Millisecond * waitTime):
		if (*opt).mode == "Test" {
			log.Info("Main - Connection dropped - ", currentConnection)
		}
		changeConnectionChan <- ""
		(*opt).configurationChan <- msg
	}
}

func manageActions(opt *options) {
	currentConnection := (*opt).firstLeader
	changeConnectionChan := make(chan raft.ServerID)
	var actions []engine.GameLog
	var clearToSend = true
	actionDoneChannel := make(chan bool)
	for {
		select {
		case msg := <-(*opt).actionChan:
			if len(actions) < 256 {
				actions = append(actions, msg)
			} else {
				log.Debug("Too many actions, drop one")
			}
		case msg := <-(*opt).configurationChan:
			var configurationResponse raft.AddRemoveServerResponse
			var configurationArgs = raft.AddRemoveServerArgs{(*opt).id, msg, []byte{}}
			configurationArgs.Signature = getConfigurationArgsSignature((*opt).clientPrivateKey, &configurationArgs)
			var conn, found = (*(*opt).connections).Load(currentConnection)
			if !found {
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
				go handleConfigurationResponse(nil, nil, changeConnectionChan, msg, currentConnection, opt)
				(*opt).requestNewServerIDChan <- true
				currentConnection = <-(*opt).getNewServerIDChan
			} else {
				log.Info("Main - Send connection request to ", currentConnection)
				var raftConn = conn.(raft.RaftConnection)
				actionCall := raftConn.Connection.Go("RaftListener.AddRemoveServerRPC", &configurationArgs, &configurationResponse, nil)
				go handleConfigurationResponse(actionCall, &configurationResponse, changeConnectionChan, msg, currentConnection, opt)
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
				(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
			}
		case done := <-actionDoneChannel:
			clearToSend = true
			if !done {
				var updateLeaderArgs = raft.UpdateLeaderArgs{string((*opt).id), raft.ServerID(currentConnection), []byte{}}
				updateLeaderArgs.Signature = getUpdateLeaderSignature((*opt).clientPrivateKey, &updateLeaderArgs)
				broadcastUpdateLeaderRPC(opt, &updateLeaderArgs, changeConnectionChan)
			}
			if done && len(actions) > 0 {
				if actions[0].Action.Action == engine.DISCONNECT {
					(*opt).removedFromGameChan <- true
				}
				copy(actions, actions[1:])
				actions = actions[:len(actions)-1]
			}
		case <-time.After(time.Millisecond * 20):
			if clearToSend && len(actions) > 0 {
				clearToSend = false
				var msg = actions[0]
				var timestamp = getNowMs()
				// var actionResponse raft.ActionResponse
				var jsonAction, _ = json.Marshal(msg.Action)
				var actionArgs = raft.ActionArgs{string(msg.Id), msg.ActionId, msg.Type, jsonAction, []byte{}}
				actionArgs.Signature = getActionArgsSignature((*opt).clientPrivateKey, &actionArgs)
				(*opt).actionQuorum.Store(msg.ActionId, 0)
				go broadcastAction(opt, &actionArgs, timestamp, actionDoneChannel)
				/*
					var conn, found = (*(*opt).connections).Load(currentConnection)
					if !found {
						(*opt).requestConnectionChan <- raft.RequestConnection{currentConnection, (*opt).connections}
						(*opt).requestNewServerIDChan <- true
						currentConnection = <-(*opt).getNewServerIDChan
					} else {
						(*opt).actionQuorum.Store(msg.ActionId, 0)
						var raftConn = conn.(raft.RaftConnection)
						actionCall := raftConn.Connection.Go("RaftListener.ActionRPC", &actionArgs, &actionResponse, nil)
						go handleActionResponse(actionCall, &actionResponse, changeConnectionChan, msg, timestamp, currentConnection, opt, actionDoneChannel)
					}
				*/
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

	log.SetLevel(log.TraceLevel)
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
	var firstLeader = raft.ServerID("")
	// Get other servers
	otherServers := make([]raft.ServerID, 0)
	var allServers = []raft.ServerID{serverID}
	for i := 4; i < len(args); i++ {
		otherServers = append(otherServers, raft.ServerID(args[i]))
		allServers = append(allServers, raft.ServerID(args[i]))
	}

	if len(otherServers) > 0 {
		firstLeader = otherServers[0]
	} else {
		firstLeader = serverID
	}

	// Client nodeMode: UI + Engine + Raft node
	var mainConnectedChan = make(chan bool)
	var mainDisconnectedChan = make(chan bool)
	var nodeConnectedChan = make(chan bool)
	var uiActionChan = make(chan engine.GameLog)
	var uiConfChan = make(chan bool)
	var stateReqChan chan bool
	var stateChan chan engine.GameState
	var actionChan chan raft.GameLog
	var snapshotRequestChan = make(chan bool)
	var snapshotResponseChan = make(chan []byte)
	var snapshotInstallChan = make(chan []byte)
	var requestConnectionChan = make(chan raft.RequestConnection)
	var requestNewServerIDChan = make(chan bool)
	var getNewServerIDChan = make(chan raft.ServerID)
	var removedFromGameChan = make(chan bool)
	stateReqChan, stateChan, actionChan = engine.Start(playerID, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan)

	var _ = raft.Start(nodeMode, port, otherServers, actionChan, nodeConnectedChan, snapshotRequestChan, snapshotResponseChan, snapshotInstallChan, nodeKeys, clientKeys, nodePrivateKey)
	var nodeConnections = raft.ConnectToRaftServers(nil, raft.ServerID(port), otherServers)

	var opt = options{
		uiActionChan,
		uiConfChan,
		firstLeader,
		nodeConnections,
		otherServers,
		serverID,
		mainConnectedChan,
		mainDisconnectedChan,
		mode,
		requestConnectionChan,
		requestNewServerIDChan,
		getNewServerIDChan,
		clientPrivateKey,
		removedFromGameChan,
		sync.Map{},
		allServers}
	go connectionPool(&opt)
	go raft.ConnectionManager(nil, requestConnectionChan)
	go manageActions(&opt)

	if len(otherServers) > 0 {
		uiConfChan <- true
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
	<-removedFromGameChan
	uiConfChan <- false
	select {
	case <-mainDisconnectedChan:
		log.Info("Main - Shutdown complete (clean)")
	case <-time.After(time.Millisecond * 5000):
		log.Info("Main - Shutdown complete (timeout)")
	}
}
