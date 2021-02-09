package engine

import (
	"encoding/json"
	"fmt"
	"go_raft/raft"
	"strconv"
)

const (
	UP         int = 0
	RIGHT      int = 1
	DOWN       int = 2
	LEFT       int = 3
	REGISTER   int = 5
	CONNECT    int = 6
	DISCONNECT int = 7
	NOOP       int = 8
)

const MapWidth = 64
const MapHeight = 64

type engineOptions struct {
	playerID             PlayerID
	stateChan            chan []byte
	actionChan           chan raft.GameLog
	snapshotRequestChan  chan bool
	snapshotResponseChan chan []byte
	installSnapshotChan  chan []byte
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
	Players     map[PlayerID]PlayerState `json:"players"`
	CurrentTurn int                      `json:"turn"`
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

func getActionTurnFromSnapshot(state *GameState) int {
	var result = -1
	for _, value := range (*state).Players {
		if value.LastActionTurn > result {
			result = value.LastActionTurn
		}
	}
	return result
}

func checkIfTurnChanged(opt *engineOptions, state *GameState) bool {
	for _, value := range (*state).Players {
		if value.LastActionTurn != (*state).CurrentTurn {
			return false
		}
	}
	return true
}

func generateDeterministicPlayerStartingPosition(playerID PlayerID, state *GameState) Position {
	// There are MapWidth * MapHeight cells
	var idPos, _ = strconv.Atoi(fmt.Sprint(playerID))
	var nOfCells = MapWidth * MapHeight
	if idPos < nOfCells {
		idPos += nOfCells
	}
	var found = false
	var position = Position{0, 0}
	for !found {
		idPos = idPos % nOfCells
		position.X = idPos / MapWidth
		position.Y = idPos % MapWidth
		if checkCollisionWithOtherPlayers(playerID, position, state) {
			idPos++
		} else {
			found = true
		}
	}
	return position
}

func stateToString(state *GameState) string {
	var result = ""
	for id, value := range (*state).Players {
		result += fmt.Sprint(id, " - ", value.LastActionTurn)
		result += ", "
	}
	return result
}

func applyAction(state *GameState, playerID PlayerID, action ActionImpl, opt *engineOptions) {
	playerData := (*state).Players[playerID]
	var position = playerData.Pos
	fmt.Println("Apply log: ", playerID, " - ", action.Turn, " ", action)
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
		var newPosition = generateDeterministicPlayerStartingPosition(playerID, state)
		(*state).Players[playerID] = PlayerState{newPosition, action.Turn}
	}
	fmt.Println(stateToString(state))
	changed := checkIfTurnChanged(opt, state)
	fmt.Println(changed, (*state).CurrentTurn)
	if changed {
		(*state).CurrentTurn++
	}
	jsonState, _ := json.Marshal(*state)
	(*opt).stateChan <- jsonState
}

func run(opt *engineOptions) {
	var gameState = GameState{make(map[PlayerID]PlayerState), 0}
	for {
		select {
		case <-(*opt).snapshotRequestChan:
			jsonGameState, _ := json.Marshal(gameState)
			(*opt).snapshotResponseChan <- jsonGameState
		case newJsonState := <-(*opt).installSnapshotChan:
			json.Unmarshal(newJsonState, &gameState)
			gameState.CurrentTurn = getActionTurnFromSnapshot(&gameState)
		case newAction := <-(*opt).actionChan:
			var playerID = PlayerID(newAction.Id)
			var action ActionImpl
			json.Unmarshal(newAction.Action, &action)
			applyAction(&gameState, playerID, action, opt)
		}
	}
}

func Start(playerID PlayerID, snapshotRequestChan chan bool, snapshotResponseChan chan []byte, installSnapshotChan chan []byte) (chan []byte, chan raft.GameLog) {
	var stateChan = make(chan []byte)
	var actionChan = make(chan raft.GameLog)

	var options = engineOptions{
		playerID,
		stateChan,
		actionChan,
		snapshotRequestChan,
		snapshotResponseChan,
		installSnapshotChan}
	go run(&options)
	return stateChan, actionChan
}
