package ui

import (
	"encoding/json"
	"fmt"
	"go_skeletons/engine"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"
	"math/rand"
	"os"
	"sort"
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

const screenWidth = 640
const screenHeight = 480

// Field of view ~ PI/4
var fov = math.Pi / 4.0
var mapHeight = len(engine.GameMap)
var mapWidth = len(engine.GameMap[0])
var maxDepth = float64(mapHeight)
var screenSize = image.Point{screenWidth, screenHeight}
var actionCount int64 = 0

type sprite struct {
	fileName string
	img      image.Image
	height   float64
	width    float64
}

var sprites = make(map[string]*sprite)

type uiOptions struct {
	playerID    engine.PlayerID
	actionChan  chan engine.GameLog
	bot         bool
	uiStateChan chan []byte
}

type playerPosition struct {
	spr      int
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

func newRandomDirection(directionChan chan int) {
	var newDirection = rand.Intn(5)
	time.Sleep(time.Second * time.Duration(rand.Intn(4)))
	directionChan <- newDirection
}

func botBehavior(opt *uiOptions) {
	var directionChan = make(chan int)
	var direction = 0
	var currentIteration = 0
	var actionIteration = 8
	var waitingForDirection = false
	(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.REGISTER}}
	for {
		select {
		case newDirection := <-directionChan:
			direction = newDirection
			waitingForDirection = false
		case <-time.After(time.Millisecond * 25):
			if currentIteration == actionIteration {
				currentIteration = 0
				if !waitingForDirection {
					go newRandomDirection(directionChan)
					waitingForDirection = true
				}
				switch direction {
				case 0:
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.UP}}
				case 1:
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.RIGHT}}
				case 2:
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.DOWN}}
				case 3:
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.LEFT}}
				}
			} else {
				(*opt).actionChan <- engine.GameLog{(*opt).playerID, -1, "NOOP", engine.ActionImpl{engine.NOOP}}
				currentIteration++
			}
		}
	}
}

func getSpritePixel(x float64, y float64, spr *sprite) (r, g, b, a uint32) {
	var sprX = int(x * (*spr).width)
	var sprY = int(y * (*spr).height)
	return (*spr).img.At(sprX, sprY).RGBA()
}

func distance(pos1 engine.Position, pos2 engine.Position) float64 {
	return math.Sqrt(math.Pow(math.Abs(pos1.X-pos2.X), 2.0) + math.Pow(math.Abs(pos1.Y-pos2.Y), 2.0))
}

func getOrderedPlayers(state *engine.GameState, playerID engine.PlayerID) []playerPosition {
	var myPlayerPosition engine.Position
	var positions []playerPosition
	for id, data := range (*state).Players {
		if id != playerID {
			positions = append(positions, playerPosition{data.GetSprite(), data.GetPosition()})
		} else {
			myPlayerPosition = data.GetPosition()
		}
	}
	sort.Slice(positions, func(i1, i2 int) bool {
		return distance(positions[i1].position, myPlayerPosition) >= distance(positions[i2].position, myPlayerPosition)
	})
	return positions
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

func writeStateToScreen(position engine.Position, delta int64, textMem *image.RGBA, img *image.RGBA) {
	positionString := "(x: " + fmt.Sprintf("%.2f", position.X) + ", y: " + fmt.Sprintf("%.2f", position.Y) + ", a: " + fmt.Sprintf("%.2f", position.A) + ")"
	fpsString := "fps: " + fmt.Sprintf("%.2f", 1000.0/float64(delta))
	addLabel(img, 0, 15, positionString)
	addLabel(img, 0, 30, fpsString)

	draw.Draw(textMem, textMem.Bounds(), img, image.ZP, draw.Src)
}

func extractMs(t time.Time) int64 {
	return time.Duration(t.Nanosecond()).Milliseconds()
}

func paintScreen(opt *uiOptions, uiScreen screen.Screen, window screen.Window, killChan chan bool) {
	t1 := extractMs(time.Now())
	depthBuffer := make([]float64, screenWidth)
	for {
		t2 := extractMs(time.Now())
		delta := t2 - t1
		t1 = t2
		select {
		case <-killChan:
			return
		default:
		}
		buff, err := uiScreen.NewBuffer(screenSize)
		checkError(err)
		mem := buff.RGBA()

		textBuff, err := uiScreen.NewBuffer(screenSize)
		checkError(err)
		textMem := textBuff.RGBA()
		img := image.NewRGBA(image.Rect(0, 0, screenWidth, 200))

		// Get game state
		newJSONState := <-(*opt).uiStateChan
		var gameState engine.GameState
		json.Unmarshal(newJSONState, &gameState)

		// Get player position in game map
		playerData := gameState.Players[(*opt).playerID]
		playerPosition := playerData.GetPosition()
		otherPlayersPositions := getOrderedPlayers(&gameState, (*opt).playerID)
		writeStateToScreen(playerPosition, delta, textMem, img)

		for x := 0; x < screenWidth; x++ {
			var rayAngle = (playerPosition.A - fov/2.0) + (float64(x)/float64(screenWidth))*fov
			var distanceToWall = 0.0

			var hitWall = false

			var eyeX = math.Sin(rayAngle)
			var eyeY = math.Cos(rayAngle)

			for !hitWall && distanceToWall < maxDepth {
				distanceToWall += 0.1

				var testX = int(playerPosition.X + eyeX*distanceToWall)
				var testY = int(playerPosition.Y + eyeY*distanceToWall)

				// Test ray out of bounds
				if testX < 0 || testX > mapWidth || testY < 0 || testY > mapHeight {
					hitWall = true
					distanceToWall = maxDepth
				} else {
					if engine.GameMap[testX][testY] == '#' {
						hitWall = true
					}
				}
			}
			var ceiling = (float64(screenHeight) / 2.0) - float64(screenHeight)/distanceToWall
			var floor = float64(screenHeight) - ceiling

			depthBuffer[x] = distanceToWall

			// Draw walls, floor, ceiling
			for y := 0; y < screenHeight; y++ {
				if float64(y) < ceiling {
				} else if float64(y) > ceiling && float64(y) <= floor {
					var valueToRemove = uint8((150 * distanceToWall) / maxDepth)
					var color = color.RGBA{150 - valueToRemove, 150 - valueToRemove, 150 - valueToRemove, 255}
					mem.SetRGBA(x, y, color)
				} else {
					var yOffset = 1.2 - (float64(y)-float64(screenHeight)/2.0)/(float64(screenHeight/2.0))
					var valueToRemove = math.Min(yOffset*15, 15)
					var color = color.RGBA{150 - uint8(valueToRemove*10), 90 - uint8(valueToRemove*6), 15 - uint8(valueToRemove), 255}
					mem.SetRGBA(x, y, color)
				}
			}
		}

		for _, otherPosition := range otherPlayersPositions {
			otherPlayerSprite := "player_" + fmt.Sprint(otherPosition.spr)
			vecX := otherPosition.position.X - playerPosition.X
			vecY := otherPosition.position.Y - playerPosition.Y
			distanceFromPlayer := math.Sqrt(vecX*vecX + vecY*vecY)
			eyeX := math.Sin(playerPosition.A)
			eyeY := math.Cos(playerPosition.A)

			objectAngle := math.Atan2(eyeY, eyeX) - math.Atan2(vecY, vecX)

			if objectAngle < -math.Pi {
				objectAngle += 2.0 * math.Pi

			}
			if objectAngle > math.Pi {
				objectAngle -= 2.0 * math.Pi
			}

			inPlayerFOV := math.Abs(objectAngle) < fov/2.0
			if inPlayerFOV && distanceFromPlayer >= 0.5 && distanceFromPlayer < maxDepth {
				objectCeiling := screenHeight/2.0 - screenHeight/distanceFromPlayer
				objectFloor := screenHeight - objectCeiling
				objectHeight := objectFloor - objectCeiling
				objectAspectRatio := sprites[otherPlayerSprite].height / sprites[otherPlayerSprite].width
				objectWidth := objectHeight / objectAspectRatio
				middleOfObject := (0.5*(objectAngle/(fov/2.0)) + 0.5) * screenWidth
				for lx := 0; lx < int(objectWidth); lx++ {
					for ly := 0; ly < int(objectHeight); ly++ {
						sampleX := float64(lx) / objectWidth
						sampleY := float64(ly) / objectHeight
						r, g, b, a := getSpritePixel(sampleX, sampleY, sprites[otherPlayerSprite])
						objectColumn := int(middleOfObject + float64(lx) - (objectWidth / 2.0))
						if objectColumn >= 0 && objectColumn < screenWidth {
							if a > 0 && depthBuffer[objectColumn] >= distanceFromPlayer {
								mem.SetRGBA(objectColumn, int(objectCeiling)+ly, color.RGBA{uint8(r), uint8(g), uint8(b), uint8(a)})
							}
						}
					}
				}
			}
		}

		newTexture, err := uiScreen.NewTexture(screenSize)
		checkError(err)
		newTexture.Upload(image.Point{}, buff, buff.Bounds())

		textTexture, err := uiScreen.NewTexture(screenSize)
		checkError(err)
		textTexture.Upload(image.Point{}, textBuff, textBuff.Bounds())

		window.Fill(image.Rectangle{image.Point{0, 0}, image.Point{screenWidth, screenHeight + 200}}, color.RGBA{0, 0, 0, 255}, draw.Src)
		window.Copy(image.Point{}, newTexture, newTexture.Bounds(), screen.Over, nil)
		window.Copy(image.Point{0, screenHeight + 15}, textTexture, textTexture.Bounds(), screen.Over, nil)

		buff.Release()
		newTexture.Release()
		textBuff.Release()
		textTexture.Release()
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
		(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.REGISTER}}
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
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.UP}}
				} else if e.Code == key.CodeA && e.Direction == key.DirPress {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.LEFT}}
				} else if e.Code == key.CodeS && e.Direction == key.DirPress {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.DOWN}}
				} else if e.Code == key.CodeD && e.Direction == key.DirPress {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, GetActionID(), "Game", engine.ActionImpl{engine.RIGHT}}
				} else {
					(*opt).actionChan <- engine.GameLog{(*opt).playerID, -1, "NOOP", engine.ActionImpl{engine.NOOP}}
				}
			case error:
				log.Print(e)
			}
		}
	})
}

func initializeSprites() {
	sprites["player_0"] = &sprite{"/assets/player_spritesheet_0.png", nil, 64, 64}
	sprites["player_1"] = &sprite{"/assets/player_spritesheet_1.png", nil, 64, 64}
	sprites["player_2"] = &sprite{"/assets/player_spritesheet_2.png", nil, 64, 64}
	sprites["player_3"] = &sprite{"/assets/player_spritesheet_3.png", nil, 64, 64}
	sprites["player_4"] = &sprite{"/assets/player_spritesheet_4.png", nil, 64, 64}
	sprites["player_5"] = &sprite{"/assets/player_spritesheet_5.png", nil, 64, 64}
}

func loadImages() {
	dir, err := os.Getwd()
	checkError(err)
	for _, spr := range sprites {
		file, err := os.Open(dir + (*spr).fileName)
		checkError(err)
		defer file.Close()

		imageData, err := png.Decode(file)
		checkError(err)

		(*spr).img = imageData
	}
}

func Start(playerID engine.PlayerID, actionChan chan engine.GameLog, uiStateChan chan []byte, bot bool) {
	var opt = &uiOptions{
		playerID,
		actionChan,
		bot,
		uiStateChan}
	if (*opt).bot {
		go botBehavior(opt)
	} else {
		initializeSprites()
		loadImages()
		go run(opt)
	}
}
