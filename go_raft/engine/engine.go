package engine

import (
	"fmt"
	"math"
	"math/rand"
	"time"
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

type PlayerID int

type Position struct {
	// Horizontal position on map
	X float64
	// Vertical position on map
	Y float64
	// Angle of view
	A float64
}

type PlayerState struct {
	Spr int
	Pos Position
}

type Player interface {
	GetPosition() Position
}

type GameState struct {
	Players map[PlayerID]PlayerState
}

type GameLog struct {
	Id          PlayerID
	Action      int
	ChanApplied chan bool
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

func applyAction(state *GameState, action GameLog, delta float64) {
	delta = 0.01
	playerData := (*state).Players[action.Id]
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
		(*state).Players[action.Id] = PlayerState{(*state).Players[action.Id].Spr, position}
	case DOWN:
		// Move BACKWARD
		newX := position.X - (math.Sin(position.A) * playerSpeed * delta)
		newY := position.Y - (math.Cos(position.A) * playerSpeed * delta)
		hitWall := checkHitWall(newX, newY)
		if !hitWall {
			position.X = newX
			position.Y = newY
		}
		(*state).Players[action.Id] = PlayerState{(*state).Players[action.Id].Spr, position}
	case RIGHT:
		// Rotate RIGHT
		newA := position.A + (playerAngularSpeed * delta)
		position.A = newA
		(*state).Players[action.Id] = PlayerState{(*state).Players[action.Id].Spr, position}
	case LEFT:
		// Rotate LEFT
		newA := position.A - (playerAngularSpeed * delta)
		position.A = newA
		(*state).Players[action.Id] = PlayerState{(*state).Players[action.Id].Spr, position}
	case REGISTER:
		// Register new player
		(*state).Players[action.Id] = PlayerState{rand.Intn(5), Position{2.0, 2.0, 0.0}}
	}

}

func run(playerID PlayerID, requestState chan bool, stateChan chan GameState, actionChan chan GameLog, snapshotRequestChan chan bool, snapshotResponseChan chan GameState, installSnapshotChan chan GameState) {
	var gameState = GameState{make(map[PlayerID]PlayerState)}
	// gameState.Players[playerID] = PlayerState{Position{2.0, 2.0, 0.0}}
	var start = time.Now().UnixNano() / 1000000
	for {
		var now = time.Now().UnixNano() / 1000000
		var delta = float64(now-start) / 1000.0
		start = now
		select {
		case <-requestState:
			stateChan <- gameState
		case <-snapshotRequestChan:
			snapshotResponseChan <- gameState
		case newState := <-installSnapshotChan:
			fmt.Println("engine: received snapshot to install")
			gameState = newState
		case newAction := <-actionChan:
			applyAction(&gameState, newAction, delta)
		}
	}
}

func Start(playerID PlayerID, snapshotRequestChan chan bool, snapshotResponseChan chan GameState, installSnapshotChan chan GameState) (chan bool, chan GameState, chan GameLog) {
	// This channel is used by the UI to request the state of the game
	var requestState = make(chan bool)
	// This channel is used to send the state of the game to the UI
	var stateChan = make(chan GameState)
	// This channel is used to receive action updates from the Raft network
	var actionChan = make(chan GameLog)
	go run(playerID, requestState, stateChan, actionChan, snapshotRequestChan, snapshotResponseChan, installSnapshotChan)
	return requestState, stateChan, actionChan
}
