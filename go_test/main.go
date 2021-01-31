package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

type execStats struct {
	node      int
	totValue  int64
	nOfValues int64
}

var openCmds = [1024]*exec.Cmd{}
var numberOfCmds = 0

func newCommand(mode string, port int, startLeaderPort int, resChan chan execStats, idx int) {
	cmd := exec.Command("./go_raft", mode, fmt.Sprint(port), fmt.Sprint(startLeaderPort))
	cmd.Dir = "../go_raft"
	openCmds[idx] = cmd
	numberOfCmds++
	cmd.Start()
}

func killAll() {
	fmt.Println("Killall: ", numberOfCmds)
	for idx := 0; idx < numberOfCmds; idx++ {
		(*openCmds[idx]).Process.Signal(syscall.SIGTERM)
		(*openCmds[idx]).Wait()
	}
}

func readResults(node int) execStats {
	var reg = regexp.MustCompile("[0-9]+")
	var stats = execStats{node, 0, 0}
	file, _ := os.Open("/tmp/go_raft_" + fmt.Sprint(node))
	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	var fileTextLines []string

	for fileScanner.Scan() {
		fileTextLines = append(fileTextLines, fileScanner.Text())
	}

	file.Close()

	for _, eachline := range fileTextLines {
		match := reg.FindString(eachline)
		if match != "" {
			stats.nOfValues++
			var intVal, _ = strconv.Atoi(match)
			stats.totValue += int64(intVal)
		}
	}
	return stats
}

func removeAllLogFiles() {
	files, err := filepath.Glob("/tmp/go_raft_*")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
}

func testNodes(number int, retChan chan bool) {
	removeAllLogFiles()
	fmt.Println("##### Test for ", number, " nodes")
	resChan := make(chan execStats)
	// Start base nodes
	cmd := exec.Command("./go_raft", "Node", "6666")
	cmd.Dir = "../go_raft"
	openCmds[0] = cmd
	cmd.Start()
	newCommand("Node", 6667, 6666, nil, 1)
	newCommand("Node", 6668, 6666, nil, 2)
	newCommand("Node", 6669, 6666, nil, 3)
	newCommand("Node", 6670, 6666, nil, 4)
	numberOfCmds = 5

	for i := 5; i < number; i++ {
		go newCommand("Bot", 6666+i, 6666, resChan, i)
	}

	time.Sleep(time.Second * 120)
	killAll()
	fmt.Println("###################################")
	var total float64 = 0
	var nOfEntries int = 0
	var notStarted int = 0
	for i := 5; i < number; i++ {
		result := readResults(6666 + i)
		fmt.Println(result)
		if result.nOfValues > 0 {
			var localMean = float64(result.totValue) / float64(result.nOfValues)
			total += localMean
			nOfEntries++
		} else {
			notStarted++
		}
	}
	if notStarted > 0 {
		fmt.Println("!!! Some nodes did not start: ", notStarted)
	}
	var result = total / float64(nOfEntries)
	fmt.Println("Mean for ", number, " nodes: ", result)
	retChan <- true
}

func handlePrematureTermination(termChan chan os.Signal, completeChan chan bool) {
	select {
	case <-termChan:
		killAll()
		os.Exit(0)
	case <-completeChan:
	}
}

func main() {
	completeChan := make(chan bool)
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)
	go handlePrematureTermination(termChan, completeChan)

	//for i := 10; i <= 100; i += 10 {
	//	retChan := make(chan bool)
	//	go testNodes(i, retChan)
	//	<-retChan
	//}

	retChan := make(chan bool)
	go testNodes(40, retChan)
	<-retChan

	completeChan <- true
	time.Sleep(time.Second * 5)
	os.Exit(0)
}
