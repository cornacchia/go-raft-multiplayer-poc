/*
package main

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

func checkError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

const erlangCmdClientPort = "6666"

func main() {
	ServerAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+erlangCmdClientPort)
	checkError(err)

	LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	checkError(err)

	Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	checkError(err)

	defer Conn.Close()
	i := 0
	for i < 10 {
		msg := strconv.Itoa(i)
		i++
		buf := []byte(msg)
		_, err := Conn.Write(buf)
		if err != nil {
			fmt.Println(msg, err)
		}
		time.Sleep(time.Second * 1)
	}
}
*/
package main

import (
	"fmt"
	"image"
	"image/color"
	"log"

	"golang.org/x/exp/shiny/driver"
	"golang.org/x/exp/shiny/screen"
	"golang.org/x/mobile/event/key"
	"golang.org/x/mobile/event/lifecycle"
)

var (
	black    = color.RGBA{0x00, 0x00, 0x00, 0xff}
	blue0    = color.RGBA{0x00, 0x00, 0x1f, 0xff}
	blue1    = color.RGBA{0x00, 0x00, 0x3f, 0xff}
	darkGray = color.RGBA{0x3f, 0x3f, 0x3f, 0xff}
	green    = color.RGBA{0x00, 0x7f, 0x00, 0x7f}
	red      = color.RGBA{0x7f, 0x00, 0x00, 0x7f}
	yellow   = color.RGBA{0x3f, 0x3f, 0x00, 0x3f}
)

func paintScreen(w screen.Window) {
	var i = 0
	for {
		for j := 0; j < i; j++ {
			// This is how you draw pixels
			w.Fill(image.Rect(50+j, j, 51+j, 1+j), red, screen.Over)
		}
		i++
	}
}

func main() {
	// TODO: spawn goroutine to ask server state updates
	driver.Main(func(s screen.Screen) {
		w, err := s.NewWindow(&screen.NewWindowOptions{
			Title: "UI",
		})
		if err != nil {
			log.Fatal(err)
		}
		defer w.Release()
		go paintScreen(w)
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
				// TODO: spawn a goroutine to send this to the server
				if e.Code == key.CodeEscape {
					return
				} else if e.Code == key.CodeW && e.Direction == key.DirPress {
					fmt.Println("up")
				} else if e.Code == key.CodeA && e.Direction == key.DirPress {
					fmt.Println("left")
				} else if e.Code == key.CodeS && e.Direction == key.DirPress {
					fmt.Println("down")
				} else if e.Code == key.CodeD && e.Direction == key.DirPress {
					fmt.Println("right")
				}
				/*
					case paint.Event:
						fmt.Println(time.Now())
						// This is how you draw pixels
						w.Fill(image.Rect(50+i, 50, 51+i, 51), red, screen.Over)
						i++
				*/
			case error:
				log.Print(e)
			}
		}
	})
}
