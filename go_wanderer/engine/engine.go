package engine

import (
	"encoding/json"
	"go_raft/raft"
)

const (
	UP         int = 0
	RIGHT      int = 1
	DOWN       int = 2
	LEFT       int = 3
	REGISTER   int = 5
	CONNECT    int = 6
	DISCONNECT int = 7
)

const MapWidth = 64
const MapHeight = 64

type engineOptions struct {
	playerID             PlayerID
	requestState         chan bool
	stateChan            chan GameState
	actionChan           chan raft.GameLog
	snapshotRequestChan  chan bool
	snapshotResponseChan chan []byte
	installSnapshotChan  chan []byte
	currentTurnChan      chan int
}

type PlayerID string

type Position struct {
	// Horizontal position on map
	X int
	// Vertical position on map
	Y int
}

type PlayerState struct {
	Pos            Position `json:"pos"`
	LastActionTurn int      `json:"lat"`
}

type Player interface {
	GetPosition() Position
}

type GameState struct {
	Players map[PlayerID]PlayerState `json:"players"`
}

type ActionImpl struct {
	Action int `json:"act"`
	Turn   int `json:"turn"`
}

type GameLog struct {
	Id       PlayerID
	ActionId int64
	Type     string
	Action   ActionImpl
}

func (playerState *PlayerState) GetPosition() Position {
	return playerState.Pos
}

func checkCollisionWithOtherPlayers(playerID PlayerID, pos Position, state *GameState) bool {
	var result = false
	if pos.X < 0 || pos.X > MapWidth || pos.Y < 0 || pos.Y > MapHeight {
		return true
	}
	for id, value := range (*state).Players {
		if id != playerID && value.Pos.X == pos.X && value.Pos.Y == pos.Y {
			result = true
			break
		}
	}
	return result
}

func getActionTurnForNewPlayer(state *GameState) int {
	var result = 0
	for _, value := range (*state).Players {
		if result == 0 || value.LastActionTurn < result {
			result = value.LastActionTurn
		}
	}
	return result
}

func checkIfTurnChanged(state *GameState) (bool, int) {
	var result = true
	var turn = -1
	for _, value := range (*state).Players {
		if turn < 0 {
			turn = value.LastActionTurn
		} else if value.LastActionTurn != turn {
			result = false
			break
		}
	}
	return result, turn
}

func applyAction(state *GameState, playerID PlayerID, action ActionImpl, opt *engineOptions) {
	playerData := (*state).Players[playerID]
	var position = playerData.Pos
	switch action.Action {
	case UP:
		// Move UP
		newY := position.Y - 1
		hitWall := checkCollisionWithOtherPlayers(playerID, Position{position.X, newY}, state)
		if !hitWall {
			position.Y = newY
		}
		(*state).Players[playerID] = PlayerState{position, action.Turn}
	case DOWN:
		// Move DOWN
		newY := position.Y + 1
		hitWall := checkCollisionWithOtherPlayers(playerID, Position{position.X, newY}, state)
		if !hitWall {
			position.Y = newY
		}
		(*state).Players[playerID] = PlayerState{position, action.Turn}
	case RIGHT:
		// Move RIGHT
		newX := position.X + 1
		hitWall := checkCollisionWithOtherPlayers(playerID, Position{newX, position.Y}, state)
		if !hitWall {
			position.X = newX
		}
		(*state).Players[playerID] = PlayerState{position, action.Turn}
	case LEFT:
		// Move LEFT
		newX := position.X - 1
		hitWall := checkCollisionWithOtherPlayers(playerID, Position{newX, position.Y}, state)
		if !hitWall {
			position.X = newX
		}
		(*state).Players[playerID] = PlayerState{position, action.Turn}
	case REGISTER:
		// Register new player
		(*state).Players[playerID] = PlayerState{Position{0, 0}, action.Turn}
		if playerID == (*opt).playerID {
			(*opt).currentTurnChan <- (*state).Players[playerID].LastActionTurn
		}
	}
	if changed, turn := checkIfTurnChanged(state); changed {
		(*opt).currentTurnChan <- turn + 1
	}
}

func run(opt *engineOptions) {
	var gameState = GameState{make(map[PlayerID]PlayerState)}
	for {
		select {
		case <-(*opt).requestState:
			(*opt).stateChan <- gameState
		case <-(*opt).snapshotRequestChan:
			jsonGameState, _ := json.Marshal(gameState)
			(*opt).snapshotResponseChan <- jsonGameState
		case newJsonState := <-(*opt).installSnapshotChan:
			json.Unmarshal(newJsonState, &gameState)
		case newAction := <-(*opt).actionChan:
			var playerID = PlayerID(newAction.Id)
			var action ActionImpl
			json.Unmarshal(newAction.Action, &action)
			applyAction(&gameState, playerID, action, opt)
		}
	}
}

func Start(playerID PlayerID, snapshotRequestChan chan bool, snapshotResponseChan chan []byte, installSnapshotChan chan []byte, currentTurnChan chan int) (chan bool, chan GameState, chan raft.GameLog) {
	// This channel is used by the UI to request the state of the game
	var requestState = make(chan bool)
	// This channel is used to send the state of the game to the UI
	var stateChan = make(chan GameState)
	// This channel is used to receive action updates from the Raft network
	var actionChan = make(chan raft.GameLog)
	var options = engineOptions{
		playerID,
		requestState,
		stateChan,
		actionChan,
		snapshotRequestChan,
		snapshotResponseChan,
		installSnapshotChan,
		currentTurnChan}
	go run(&options)
	return requestState, stateChan, actionChan
}
