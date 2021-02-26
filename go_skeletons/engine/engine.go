package engine

import (
	"encoding/json"
	"fmt"
	"go_raft/raft"
	"math"
	"strconv"

	log "github.com/sirupsen/logrus"
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

func generateDeterministicPlayerStartingPosition(playerID PlayerID) Position {
	var idPos, _ = strconv.Atoi(fmt.Sprint(playerID))
	var mapHeight = len(GameMap)
	var mapWidth = len(GameMap[0])
	var nOfCells = mapHeight * mapWidth
	if idPos < nOfCells {
		idPos += nOfCells
	}
	var found = false
	var position = Position{0, 0, 0}
	for !found {
		idPos = idPos % nOfCells
		position.X = float64(idPos / mapWidth)
		position.Y = float64(idPos % mapWidth)
		if checkHitWall(position.X, position.Y) {
			idPos++
		} else {
			found = true
		}
	}
	return position
}

func getDeterministicPlayerSprite(playerID PlayerID) int {
	var idint, _ = strconv.Atoi(fmt.Sprint(playerID))
	return idint % 6
}

func applyAction(state *GameState, playerID PlayerID, action ActionImpl, stateChan chan []byte) {
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
		var newPosition = generateDeterministicPlayerStartingPosition(playerID)
		(*state).Players[playerID] = PlayerState{getDeterministicPlayerSprite(playerID), newPosition}
	case DISCONNECT:
		// Remove player from game
		log.Debug("Remove player from game: ", playerID)
		delete((*state).Players, playerID)
	}
	jsonState, _ := json.Marshal(*state)
	stateChan <- jsonState
}

func run(opt *engineOptions) {
	var gameState = GameState{make(map[PlayerID]PlayerState)}
	for {
		select {
		case <-(*opt).snapshotRequestChan:
			jsonGameState, _ := json.Marshal(gameState)
			(*opt).snapshotResponseChan <- jsonGameState
		case newJsonState := <-(*opt).installSnapshotChan:
			json.Unmarshal(newJsonState, &gameState)
		case newAction := <-(*opt).actionChan:
			var playerID = PlayerID(newAction.Id)
			var action ActionImpl
			json.Unmarshal(newAction.Action, &action)
			applyAction(&gameState, playerID, action, (*opt).stateChan)
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
