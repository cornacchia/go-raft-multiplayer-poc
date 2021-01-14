package engine

import "math"

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

const playerSpeed = 5
const playerAngularSpeed = 1.5
const delta = 0.2

func (playerState *PlayerState) GetPosition() (float64, float64, float64) {
	return playerState.pos.x, playerState.pos.y, playerState.pos.a
}

func checkHitWall(position Position) bool {
	return GameMap[int(math.Floor(position.x+1))][int(math.Floor(position.y+1))] == '#'
}

// This function modifies the state of the game
// it will modify the state for performance
func applyAction(state *GameState, action GameLog) {
	var playerData = state.Players[action.Id]
	var position = playerData.pos
	switch action.Action {
	case 0:
		// Move UP
		newX := position.x + (math.Sin(position.a) * playerSpeed * delta)
		newY := position.y + (math.Cos(position.a) * playerSpeed * delta)
		hitWall := checkHitWall(position)
		if !hitWall {
			position.x = newX
			position.y = newY
		}
	case 2:
		// Move DOWN
		newX := position.x - (math.Sin(position.a) * playerSpeed * delta)
		newY := position.y - (math.Cos(position.a) * playerSpeed * delta)
		hitWall := checkHitWall(position)
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
}

func run(playerID PlayerID, requestState chan bool, stateChan chan GameState, actionChan chan GameLog) {
	var gameState GameState
	for {
		select {
		case <-requestState:
			stateChan <- gameState
		case newAction := <-actionChan:
			applyAction(&gameState, newAction)
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
