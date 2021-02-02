package engine

import (
	"encoding/json"
	"fmt"
	"go_raft/raft"
	"math"
	"math/rand"
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

type engineOptions struct {
	playerID             PlayerID
	requestState         chan bool
	stateChan            chan GameState
	actionChan           chan raft.GameLog
	snapshotRequestChan  chan bool
	snapshotResponseChan chan []byte
	installSnapshotChan  chan []byte
}

type PlayerID string

type Position struct {
	// Horizontal position on map
	X float64
	// Vertical position on map
	Y float64
	// Angle of view
	A float64
}

type PlayerState struct {
	Spr int      `json:"spr"`
	Pos Position `json:"pos"`
}

type Player interface {
	GetPosition() Position
}

type GameState struct {
	Players map[PlayerID]PlayerState `json:"players"`
}

type ActionImpl struct {
	Action int `json:"act"`
}

type GameLog struct {
	Id       PlayerID
	ActionId int64
	Type     string
	Action   ActionImpl
}

var GameMap = [16]string{
	"################",
	"#..............#",
	"#.....#........#",
	"#.....#........#",
	"#.....#........#",
	"#######........#",
	"#..............#",
	"#..............#",
	"#......#########",
	"#......#.......#",
	"#......#.......#",
	"#......#.......#",
	"#..............#",
	"#..............#",
	"#..............#",
	"################"}

const playerSpeed = 10
const playerAngularSpeed = 5

func (playerState *PlayerState) GetPosition() Position {
	return playerState.Pos
}

func (playerState *PlayerState) GetSprite() int {
	return playerState.Spr
}

func checkHitWall(x float64, y float64) bool {
	return GameMap[int(x)][int(y)] == '#'
}

// This function modifies the state of the game
// it will modify the state for performance

func applyAction(state *GameState, playerID PlayerID, action ActionImpl) {
	var delta = 0.01
	playerData := (*state).Players[playerID]
	var position = playerData.Pos
	switch action.Action {
	case UP:
		// Move FORWARD
		newX := position.X + (math.Sin(position.A) * playerSpeed * delta)
		newY := position.Y + (math.Cos(position.A) * playerSpeed * delta)
		hitWall := checkHitWall(newX, newY)
		if !hitWall {
			position.X = newX
			position.Y = newY
		}
		(*state).Players[playerID] = PlayerState{(*state).Players[playerID].Spr, position}
	case DOWN:
		// Move BACKWARD
		newX := position.X - (math.Sin(position.A) * playerSpeed * delta)
		newY := position.Y - (math.Cos(position.A) * playerSpeed * delta)
		hitWall := checkHitWall(newX, newY)
		if !hitWall {
			position.X = newX
			position.Y = newY
		}
		(*state).Players[playerID] = PlayerState{(*state).Players[playerID].Spr, position}
	case RIGHT:
		// Rotate RIGHT
		newA := position.A + (playerAngularSpeed * delta)
		position.A = newA
		(*state).Players[playerID] = PlayerState{(*state).Players[playerID].Spr, position}
	case LEFT:
		// Rotate LEFT
		newA := position.A - (playerAngularSpeed * delta)
		position.A = newA
		(*state).Players[playerID] = PlayerState{(*state).Players[playerID].Spr, position}
	case REGISTER:
		// Register new player
		(*state).Players[playerID] = PlayerState{rand.Intn(5), Position{2.0, 2.0, 0.0}}
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
			fmt.Println("engine: received snapshot to install")
			json.Unmarshal(newJsonState, &gameState)
		case newAction := <-(*opt).actionChan:
			var playerID = PlayerID(newAction.Id)
			var action ActionImpl
			json.Unmarshal(newAction.Action, &action)
			applyAction(&gameState, playerID, action)
		}
	}
}

func Start(playerID PlayerID, snapshotRequestChan chan bool, snapshotResponseChan chan []byte, installSnapshotChan chan []byte) (chan bool, chan GameState, chan raft.GameLog) {
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
		installSnapshotChan}
	go run(&options)
	return requestState, stateChan, actionChan
}
