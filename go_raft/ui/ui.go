package ui

import (
	"fmt"
	"go_raft/engine"
	"go_raft/raft"
	"image"
	"image/color"
	"log"
	"math"
	"net"
	"strconv"

	"golang.org/x/exp/shiny/driver"
	"golang.org/x/exp/shiny/screen"
	"golang.org/x/mobile/event/key"
	"golang.org/x/mobile/event/lifecycle"
)

const screenWidth = 640
const screenHeight = 480

// Field of view ~ PI/4
var fov = 3.14159 / 4.0
var mapHeight = len(engine.GameMap)
var mapWidth = len(engine.GameMap[0])
var maxDepth = float64(mapHeight)
var screenSize = image.Point{screenWidth, screenHeight}

type uiOptions struct {
	playerID          engine.PlayerID
	stateRequestChan  chan bool
	gameStateChan     chan engine.GameState
	otherServers      []raft.ServerID
	currentConnection *net.UDPConn
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

func sendCmd(conn *net.UDPConn, playerID engine.PlayerID, cmd int) {
	msg := string(playerID) + "|" + strconv.Itoa(cmd)
	buf := []byte(msg)
	_, err := conn.Write(buf)
	checkError(err)
}

func setupUDPConnection(otherServers []raft.ServerID) *net.UDPConn {
	// take a random server from otherServers
	// if its the leader it will respond OK
	// otherwise it will give the addr of the current leader
	// keep trying until you get the address
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+port)
	checkError(err)
	localAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	checkError(err)
	conn, err := net.DialUDP("udp", localAddr, addr)
	checkError(err)
	// TODO: somewhere conn.Close()
	return conn
}

func paintScreen(opt *uiOptions, uiScreen screen.Screen, window screen.Window) {
	for {
		buff, err := uiScreen.NewBuffer(screenSize)
		checkError(err)
		mem := buff.RGBA()

		// Get game state
		(*opt).stateRequestChan <- true
		gameState := <-(*opt).gameStateChan
		// Get player position in game map
		playerData := gameState.Players[(*opt).playerID]
		playerX, playerY, playerA := playerData.GetPosition()

		for x := 0; x < screenWidth; x++ {
			var rayAngle = (playerA - fov/2.0) + (float64(x)/float64(screenWidth))*fov
			var distanceToWall = 0.0

			var hitWall = false

			var eyeX = math.Sin(rayAngle)
			var eyeY = math.Cos(rayAngle)

			for !hitWall && distanceToWall < maxDepth {
				distanceToWall += 0.1

				var testX = int(playerX + eyeX*distanceToWall)
				var testY = int(playerY + eyeY*distanceToWall)

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

			for y := 0; y < screenHeight; y++ {
				mem.SetRGBA(x, y, color.RGBA{0, 0, 0, 255})
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
		newTexture, err := uiScreen.NewTexture(screenSize)
		checkError(err)
		newTexture.Upload(image.Point{}, buff, buff.Bounds())
		window.Copy(image.Point{}, newTexture, newTexture.Bounds(), screen.Over, nil)
		buff.Release()
		newTexture.Release()
	}
}

func run(opt *uiOptions) {
	driver.Main(func(uiScreen screen.Screen) {
		window, err := uiScreen.NewWindow(&screen.NewWindowOptions{
			Title: "UI",
		})
		checkError(err)
		defer window.Release()

		go paintScreen(opt, uiScreen, window)

		for {
			e := window.NextEvent()

			// Print events
			/*
				format := "got %#v\n"
				if _, ok := e.(fmt.Stringer); ok {
					format = "got %v\n"
				}
				fmt.Printf(format, e)
			*/
			switch e := e.(type) {
			case lifecycle.Event:
				if e.To == lifecycle.StageDead {
					return
				}

			case key.Event:
				if e.Code == key.CodeEscape {
					return
				} else if e.Code == key.CodeW && e.Direction == key.DirPress {
					go sendCmd((*opt).currentConnection, (*opt).playerID, 0)
				} else if e.Code == key.CodeA && e.Direction == key.DirPress {
					go sendCmd((*opt).currentConnection, (*opt).playerID, 3)
				} else if e.Code == key.CodeS && e.Direction == key.DirPress {
					go sendCmd((*opt).currentConnection, (*opt).playerID, 2)
				} else if e.Code == key.CodeD && e.Direction == key.DirPress {
					go sendCmd((*opt).currentConnection, (*opt).playerID, 1)
				}
			case error:
				log.Print(e)
			}
		}
	})
}

func Start(playerID engine.PlayerID, stateRequestChan chan bool, gameStateChan chan engine.GameState, otherServers []raft.ServerID) {
	var cmdConn = setupUDPConnection(otherServers)
	defer cmdConn.Close()

	var opt = &uiOptions{
		playerID,
		stateRequestChan,
		gameStateChan,
		otherServers,
		cmdConn}
	go run(opt)
}
