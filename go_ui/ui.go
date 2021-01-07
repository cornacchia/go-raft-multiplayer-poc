package main

import (
	"bufio"
	"fmt"
	"image"
	"image/color"
	"log"
	"math"
	"net"
	"strconv"
	"strings"

	"golang.org/x/exp/shiny/driver"
	"golang.org/x/exp/shiny/screen"
	"golang.org/x/mobile/event/key"
	"golang.org/x/mobile/event/lifecycle"
)

const cmdPort = "6666"
const statePort = "6667"

// Player data type
type Player struct {
	id string
	x  float64
	y  float64
	a  float64
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

func generateMap(messages []string) [][]byte {
	mapData := make([][]byte, 16)
	for r := 0; r < 16; r++ {
		mapData[r] = make([]byte, 16)
	}
	var mIdx = 0
	for i := 0; i < 16; i++ {
		for j := 0; j < 16; j++ {
			mapData[i][j] = messages[1][mIdx]
			mIdx++
		}
	}
	return mapData
}

func parsePlayer(messages []string) Player {
	var x float64
	var y float64
	var a float64
	var id string
	var stringParts = strings.Split(messages[1], "|")
	id = stringParts[0]
	x, _ = strconv.ParseFloat(stringParts[1], 32)
	y, _ = strconv.ParseFloat(stringParts[2], 32)
	a, _ = strconv.ParseFloat(stringParts[3], 32)
	return Player{id, x, y, a}
}

func getState(conn *net.UDPConn, mapRequest chan bool, mapChan chan [][]byte, playerRequest chan bool, playerChan chan Player, chanGotMap chan bool, chanGotPlayer chan bool) {
	var gotMap = false
	var gotPlayer = false
	var mapData [][]byte
	var player Player
	for {
		select {
		case <-mapRequest:
			mapChan <- mapData
		case <-playerRequest:
			playerChan <- player
		default:
			var msg = strconv.Itoa(1)
			if !gotMap {
				msg = strconv.Itoa(0)
			}
			buf := []byte(msg)
			_, err := conn.Write(buf)
			checkError(err)
			if !gotMap {
				var gotAll = false
				messages := []string{}
				for !gotAll {
					message, _ := bufio.NewReader(conn).ReadString('\n')
					messages = append(messages, message)
					if message == "Map_End\n" {
						gotAll = true
					}
				}
				mapData = generateMap(messages)
				gotMap = true
				chanGotMap <- true
			} else {
				var gotAll = false
				messages := []string{}
				for !gotAll {
					message, _ := bufio.NewReader(conn).ReadString('\n')
					messages = append(messages, message)
					if message == "Player_End\n" {
						gotAll = true
					}
				}
				player = parsePlayer(messages)
				if !gotPlayer {
					gotPlayer = true
					chanGotPlayer <- true
				}

			}
		}
	}
}

func sendCmd(conn *net.UDPConn, cmd int) {
	msg := strconv.Itoa(cmd)
	buf := []byte(msg)
	_, err := conn.Write(buf)
	checkError(err)
}

func setupUDPConnection(port string) *net.UDPConn {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+port)
	checkError(err)
	localAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	checkError(err)
	conn, err := net.DialUDP("udp", localAddr, addr)
	checkError(err)
	return conn
}

func paintScreen(s screen.Screen, w screen.Window, mapRequest chan bool, mapChan chan [][]byte, playerRequest chan bool, playerChan chan Player) {
	var screenWidth = 640
	var screenHeight = 480
	var mapWidth = 16
	var mapHeight = 16
	var maxDepth = 16.0
	var fov = 3.14159 / 4.0
	mapRequest <- true
	mapData := <-mapChan
	var screenSize = image.Point{screenWidth, screenHeight}
	for {
		b, err := s.NewBuffer(screenSize)
		m := b.RGBA()
		checkError(err)
		playerRequest <- true
		playerData := <-playerChan
		for x := 0; x < screenWidth; x++ {
			var rayAngle = (playerData.a - fov/2.0) + (float64(x)/float64(screenWidth))*fov
			var distanceToWall = 0.0

			var hitWall = false

			var eyeX = math.Sin(rayAngle)
			var eyeY = math.Cos(rayAngle)

			for !hitWall && distanceToWall < maxDepth {
				distanceToWall += 0.1

				var testX = int(playerData.x + eyeX*distanceToWall)
				var testY = int(playerData.y + eyeY*distanceToWall)

				// Test ray out of bounds
				if testX < 0 || testX > mapWidth || testY < 0 || testY > mapHeight {
					hitWall = true
					distanceToWall = maxDepth
				} else {
					if mapData[testX][testY] == '#' {
						hitWall = true
					}
				}
			}
			var ceiling = (float64(screenHeight) / 2.0) - float64(screenHeight)/distanceToWall
			var floor = float64(screenHeight) - ceiling

			for y := 0; y < screenHeight; y++ {
				m.SetRGBA(x, y, color.RGBA{0, 0, 0, 255})
				if float64(y) < ceiling {
				} else if float64(y) > ceiling && float64(y) <= floor {
					var valueToRemove = uint8((150 * distanceToWall) / maxDepth)
					var color = color.RGBA{150 - valueToRemove, 150 - valueToRemove, 150 - valueToRemove, 255}
					m.SetRGBA(x, y, color)
				} else {
					var xOffset = 0.4 * (math.Abs(float64(screenWidth)/2.0 - float64(x))) / (float64(screenWidth) / 2.0)
					var yOffset = 0.6 * (1.0 - (float64(y)-float64(screenHeight)/2.0)/(float64(screenHeight/2.0)))
					var valueToRemove = (yOffset + xOffset) * 15
					var color = color.RGBA{150 - uint8(valueToRemove*10), 90 - uint8(valueToRemove*6), 15 - uint8(valueToRemove), 255}
					m.SetRGBA(x, y, color)
				}
			}
		}
		t0, err := s.NewTexture(screenSize)
		checkError(err)
		t0.Upload(image.Point{}, b, b.Bounds())
		w.Copy(image.Point{}, t0, t0.Bounds(), screen.Over, nil)
		b.Release()
		t0.Release()
	}
}

func main() {
	var cmdConn = setupUDPConnection(cmdPort)
	defer cmdConn.Close()
	var stateConn = setupUDPConnection(statePort)
	defer stateConn.Close()
	mapChan := make(chan [][]byte)
	mapRequest := make(chan bool)
	playerChan := make(chan Player)
	playerRequest := make(chan bool)
	gotMap := make(chan bool)
	gotPlayer := make(chan bool)
	go getState(stateConn, mapRequest, mapChan, playerRequest, playerChan, gotMap, gotPlayer)

	driver.Main(func(s screen.Screen) {
		w, err := s.NewWindow(&screen.NewWindowOptions{
			Title: "UI",
		})
		if err != nil {
			log.Fatal(err)
		}
		defer w.Release()
		<-gotMap
		<-gotPlayer
		go paintScreen(s, w, mapRequest, mapChan, playerRequest, playerChan)
		for {
			e := w.NextEvent()

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
					go sendCmd(cmdConn, 0)
				} else if e.Code == key.CodeA && e.Direction == key.DirPress {
					go sendCmd(cmdConn, 3)
				} else if e.Code == key.CodeS && e.Direction == key.DirPress {
					go sendCmd(cmdConn, 2)
				} else if e.Code == key.CodeD && e.Direction == key.DirPress {
					go sendCmd(cmdConn, 1)
				}
			case error:
				log.Print(e)
			}
		}
	})
}
