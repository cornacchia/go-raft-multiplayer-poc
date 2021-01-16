package engine

import (
	"math"
	"time"
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
	pos Position
}

type Player interface {
	GetPosition() Position
}

type GameState struct {
	Players map[PlayerID]PlayerState
}

type GameLog struct {
	Id     PlayerID
	Action int
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
	return playerState.pos
}

func checkHitWall(x float64, y float64) bool {
	return GameMap[int(x)][int(y)] == '#'
}

// This function modifies the state of the game
// it will modify the state for performance

// TODO probabilmente mandare una azione iniziale di iscrizione
func applyAction(state *GameState, action GameLog, delta float64) {
	delta = 0.01
	playerData := (*state).Players[action.Id]
	var position = playerData.pos
	switch action.Action {
	case 0:
		// Move FORWARD
		newX := position.X + (math.Sin(position.A) * playerSpeed * delta)
		newY := position.Y + (math.Cos(position.A) * playerSpeed * delta)
		hitWall := checkHitWall(newX, newY)
		if !hitWall {
			position.X = newX
			position.Y = newY
		}
		(*state).Players[action.Id] = PlayerState{position}
	case 2:
		// Move BACKWARD
		newX := position.X - (math.Sin(position.A) * playerSpeed * delta)
		newY := position.Y - (math.Cos(position.A) * playerSpeed * delta)
		hitWall := checkHitWall(newX, newY)
		if !hitWall {
			position.X = newX
			position.Y = newY
		}
		(*state).Players[action.Id] = PlayerState{position}
	case 1:
		// Rotate RIGHT
		newA := position.A + (playerAngularSpeed * delta)
		position.A = newA
		(*state).Players[action.Id] = PlayerState{position}
	case 3:
		// Rotate LEFT
		newA := position.A - (playerAngularSpeed * delta)
		position.A = newA
		(*state).Players[action.Id] = PlayerState{position}
	case 5:
		// Register new player
		(*state).Players[action.Id] = PlayerState{Position{2.0, 2.0, 0.0}}
	}

}

func run(playerID PlayerID, requestState chan bool, stateChan chan GameState, actionChan chan GameLog) {
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
		case newAction := <-actionChan:
			applyAction(&gameState, newAction, delta)
		}
	}
}

func Start(playerID PlayerID) (chan bool, chan GameState, chan GameLog) {
	// This channel is used by the UI to request the state of the game
	var requestState = make(chan bool)
	// This channel is used to send the state of the game to the UI
	var stateChan = make(chan GameState)
	// This channel is used to receive action updates from the Raft network
	var actionChan = make(chan GameLog)
	go run(playerID, requestState, stateChan, actionChan)
	return requestState, stateChan, actionChan
}
