package ui

import (
	"encoding/json"
	"fmt"
	"go_wanderer/engine"
	"image"
	"image/color"
	"image/draw"
	"math/rand"
	"time"

	"golang.org/x/exp/shiny/driver"
	"golang.org/x/exp/shiny/screen"
	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/math/fixed"
	"golang.org/x/mobile/event/key"
	"golang.org/x/mobile/event/lifecycle"

	log "github.com/sirupsen/logrus"
)

const squareDimension = 15
const screenWidth = engine.MapWidth * squareDimension
const screenHeight = engine.MapHeight * squareDimension

var screenSize = image.Point{screenWidth, screenHeight}
var actionCount int64 = 0

type uiOptions struct {
	playerID              engine.PlayerID
	actionChan            chan engine.GameLog
	bot                   bool
	uiStateChan           chan []byte
	currentTurnUIChan     chan int
	currentTurnEngineChan chan int
}

type playerPosition struct {
	position engine.Position
}

func checkError(err error) {
	if err != nil {
		log.Error("Error: ", err)
	}
}

func GetActionID() int64 {
	actionCount++
	return actionCount
}

func verifyClearToSend(opt *uiOptions, currentTurn int) (bool, int) {
	var clearToSend = false
	(*opt).currentTurnUIChan <- 1
	newTurn := <-(*opt).currentTurnUIChan
	if newTurn != currentTurn {
		clearToSend = true
	}
	return clearToSend, newTurn
}

func getCurrentTurn(opt *uiOptions) {
	for {
		// Get game state
		newJSONState := <-(*opt).uiStateChan
		var gameState engine.GameState
		json.Unmarshal(newJSONState, &gameState)
		(*opt).currentTurnEngineChan <- gameState.CurrentTurn
	}
}

func botBehavior(opt *uiOptions) {
	var direction = 0
	var currentIteration = 0
	var waitIteration = 0
	var actionIteration = 4
	go getCurrentTurn(opt)
	(*opt).currentTurnUIChan <- 1
	currentTurn := <-(*opt).currentTurnUIChan
	(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.REGISTER, currentTurn}}
	for {
		time.Sleep(time.Millisecond * 25)
		if currentIteration == actionIteration && waitIteration >= 400 {
			currentIteration = 0
			direction = rand.Intn(5)
			switch direction {
			case 0:
				if clear, newTurn := verifyClearToSend(opt, currentTurn); clear {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.UP, newTurn}}
					currentTurn = newTurn
					waitIteration = 0
				}
			case 1:
				if clear, newTurn := verifyClearToSend(opt, currentTurn); clear {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.RIGHT, newTurn}}
					currentTurn = newTurn
					waitIteration = 0
				}
			case 2:
				if clear, newTurn := verifyClearToSend(opt, currentTurn); clear {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.DOWN, newTurn}}
					currentTurn = newTurn
					waitIteration = 0
				}
			case 3:
				if clear, newTurn := verifyClearToSend(opt, currentTurn); clear {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.LEFT, newTurn}}
					currentTurn = newTurn
					waitIteration = 0
				}
			}
		} else {
			(*opt).actionChan <- engine.GameLog{(*opt).playerID, -1, "NOOP", engine.ActionImpl{engine.NOOP, 0}}
			currentIteration++
			waitIteration++
		}
	}
}

func addLabel(img *image.RGBA, x, y int, label string) {
	col := color.RGBA{255, 255, 255, 255}
	point := fixed.Point26_6{fixed.Int26_6(x * 64), fixed.Int26_6(y * 64)}

	d := &font.Drawer{
		Dst:  img,
		Src:  image.NewUniform(col),
		Face: basicfont.Face7x13,
		Dot:  point,
	}
	d.DrawString(label)
}

func getOtherPlayers(state *engine.GameState, playerID engine.PlayerID) []playerPosition {
	var positions []playerPosition
	for id, data := range (*state).Players {
		if id != playerID {
			positions = append(positions, playerPosition{data.GetPosition()})
		}
	}
	return positions
}

func drawPlayer(color color.RGBA, pos playerPosition, mem *image.RGBA) {
	for i := 0; i < squareDimension; i++ {
		for j := 0; j < squareDimension; j++ {
			mem.SetRGBA((pos.position.X*squareDimension)+i, (pos.position.Y*squareDimension)+j, color)
		}
	}
}

func paintScreen(opt *uiOptions, uiScreen screen.Screen, window screen.Window, killChan chan bool) {
	var playerColor = color.RGBA{255, 0, 0, 255}
	var otherColor = color.RGBA{0, 0, 255, 255}
	for {
		select {
		case <-killChan:
			return
		default:
		}
		buff, err := uiScreen.NewBuffer(screenSize)
		checkError(err)
		mem := buff.RGBA()

		// Get game state
		newJSONState := <-(*opt).uiStateChan
		var gameState engine.GameState
		json.Unmarshal(newJSONState, &gameState)
		(*opt).currentTurnEngineChan <- gameState.CurrentTurn

		// Get player position in game map
		playerData := gameState.Players[(*opt).playerID]
		playerPos := playerPosition{playerData.GetPosition()}

		otherPlayersPositions := getOtherPlayers(&gameState, (*opt).playerID)

		drawPlayer(playerColor, playerPos, mem)
		for _, otherPosition := range otherPlayersPositions {
			drawPlayer(otherColor, otherPosition, mem)
		}

		newTexture, err := uiScreen.NewTexture(screenSize)
		checkError(err)
		newTexture.Upload(image.Point{}, buff, buff.Bounds())

		window.Fill(image.Rectangle{image.Point{0, 0}, image.Point{screenWidth, screenHeight}}, color.RGBA{0, 0, 0, 255}, draw.Src)
		window.Copy(image.Point{}, newTexture, newTexture.Bounds(), screen.Over, nil)

		buff.Release()
		newTexture.Release()
	}
}

func keepRefreshing(window *screen.Window) {
	for {
		time.Sleep(time.Millisecond * 25)
		var evt key.Event
		(*window).Send(evt)
	}
}

func run(opt *uiOptions) {
	driver.Main(func(uiScreen screen.Screen) {
		window, err := uiScreen.NewWindow(&screen.NewWindowOptions{
			Title:  "UI - " + fmt.Sprint((*opt).playerID),
			Width:  screenWidth,
			Height: screenHeight + 200,
		})
		checkError(err)
		defer window.Release()

		killChan := make(chan bool)
		go paintScreen(opt, uiScreen, window, killChan)
		go keepRefreshing(&window)
		(*opt).currentTurnUIChan <- 1
		currentTurn := <-(*opt).currentTurnUIChan
		(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.REGISTER, currentTurn}}
		for {
			e := window.NextEvent()

			switch e := e.(type) {
			case lifecycle.Event:
				if e.To == lifecycle.StageDead {
					killChan <- true
					return
				}

			case key.Event:
				if e.Code == key.CodeEscape {
					killChan <- true
					return
				} else if e.Code == key.CodeW && e.Direction == key.DirPress {
					if clear, newTurn := verifyClearToSend(opt, currentTurn); clear {
						(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.UP, newTurn}}
						currentTurn = newTurn
					}
				} else if e.Code == key.CodeA && e.Direction == key.DirPress {
					if clear, newTurn := verifyClearToSend(opt, currentTurn); clear {
						(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.LEFT, newTurn}}
						currentTurn = newTurn
					}
				} else if e.Code == key.CodeS && e.Direction == key.DirPress {
					if clear, newTurn := verifyClearToSend(opt, currentTurn); clear {
						(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.DOWN, newTurn}}
						currentTurn = newTurn
					}
				} else if e.Code == key.CodeD && e.Direction == key.DirPress {
					if clear, newTurn := verifyClearToSend(opt, currentTurn); clear {
						(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.RIGHT, newTurn}}
						currentTurn = newTurn
					}
				} else {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, -1, "NOOP", engine.ActionImpl{engine.NOOP, 0}}
				}
			case error:
				log.Print(e)
			}
		}
	})
}

// TODO goroutine che gestisca in modo centralizzato il turno

func Start(playerID engine.PlayerID, actionChan chan engine.GameLog, uiStateChan chan []byte, currentTurnUIChan chan int, currentTurnEngineChan chan int, bot bool) {
	var opt = &uiOptions{
		playerID,
		actionChan,
		bot,
		uiStateChan,
		currentTurnUIChan,
		currentTurnEngineChan}
	if (*opt).bot {
		go botBehavior(opt)
	} else {
		go run(opt)
	}
}
