package engine

import (
	"math"
	"time"
)

type PlayerID int

type Position struct {
	// Horizontal position on map
	x float64
	// Vertical position on map
	y float64
	// Angle of view
	a float64
}

type PlayerState struct {
	pos Position
}

type Player interface {
	GetPosition() (float64, float64, float64)
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

func (playerState *PlayerState) GetPosition() (float64, float64, float64) {
	return playerState.pos.x, playerState.pos.y, playerState.pos.a
}

func checkHitWall(x float64, y float64) bool {
	return GameMap[int(x)][int(y)] == '#'
}

// This function modifies the state of the game
// it will modify the state for performance
func applyAction(state *GameState, action GameLog, delta float64) {
	var playerData = (*state).Players[action.Id]
	var position = playerData.pos
	switch action.Action {
	case 0:
		// Move FORWARD
		newX := position.x + (math.Sin(position.a) * playerSpeed * delta)
		newY := position.y + (math.Cos(position.a) * playerSpeed * delta)
		hitWall := checkHitWall(newX, newY)
		if !hitWall {
			position.x = newX
			position.y = newY
		}
	case 2:
		// Move BACKWARD
		newX := position.x - (math.Sin(position.a) * playerSpeed * delta)
		newY := position.y - (math.Cos(position.a) * playerSpeed * delta)
		hitWall := checkHitWall(newX, newY)
		if !hitWall {
			position.x = newX
			position.y = newY
		}
	case 1:
		// Rotate RIGHT
		newA := position.a + (playerAngularSpeed * delta)
		position.a = newA
	case 3:
		// Rotate LEFT
		newA := position.a - (playerAngularSpeed * delta)
		position.a = newA
	}
	(*state).Players[action.Id] = PlayerState{position}
}

func run(playerID PlayerID, requestState chan bool, stateChan chan GameState, actionChan chan GameLog, start int64) {
	var gameState = GameState{make(map[PlayerID]PlayerState)}
	gameState.Players[playerID] = PlayerState{Position{2.0, 2.0, 0.0}}
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
	var now = time.Now().UnixNano() / 1000000
	go run(playerID, requestState, stateChan, actionChan, now)
	return requestState, stateChan, actionChan
}
