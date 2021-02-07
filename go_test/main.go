package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

const timeToWait = 180

type execStats struct {
	node      int
	totValue  int64
	nOfValues int64
	dropped   int64
}

var openCmds = [1024]*exec.Cmd{}
var numberOfCmds = 0

func baseNode(pkgToTest string) {
	cmd := exec.Command("./"+pkgToTest, "Test", "Node", "6666")
	cmd.Dir = "../" + pkgToTest
	// cmd.Stderr = os.Stderr
	openCmds[0] = cmd
	numberOfCmds = 1
	cmd.Start()
	//if err := cmd.Wait(); err != nil {
	//	killAll()
	//	log.Fatal(err)
	//}
	cmd.Wait()
}

func newCommand(pkgToTest string, mode string, port int, startLeaderPort int, idx int) {
	cmd := exec.Command("./"+pkgToTest, "Test", mode, fmt.Sprint(port), fmt.Sprint(startLeaderPort))
	cmd.Dir = "../" + pkgToTest
	// cmd.Stderr = os.Stderr
	openCmds[idx] = cmd
	numberOfCmds++
	cmd.Start()
	//if err := cmd.Wait(); err != nil {
	//	killAll()
	//	log.Fatal(err)
	//}
	cmd.Wait()
}

func killAll() {
	fmt.Println("Killall: ", numberOfCmds)
	for idx := 0; idx < numberOfCmds; idx++ {
		(*openCmds[idx]).Process.Signal(syscall.SIGTERM)
		(*openCmds[idx]).Wait()
	}
}

func analyzeNodeBehavior(node int) {
	fmt.Printf("###################### %d #####################\n", node)
	var lastRaftLogReceived = -1
	var lastTurn = -1
	var newTurnRegex = regexp.MustCompile("New turn: ([0-9]+)")
	var raftLogIdxRegex = regexp.MustCompile("Raft apply log: ([0-9]+)")
	var stateSnapshotIdxRegex = regexp.MustCompile("State: install snapshot ([0-9]+)")
	var shuttingDownRegex = regexp.MustCompile("Shutting down...")

	file, _ := os.Open("/tmp/go_raft_log_" + fmt.Sprint(node))
	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	var fileTextLines []string

	for fileScanner.Scan() {
		fileTextLines = append(fileTextLines, fileScanner.Text())
	}

	file.Close()

	for _, line := range fileTextLines {
		var matchLogIdx = raftLogIdxRegex.FindStringSubmatch(line)
		var matchSnapshotIdx = stateSnapshotIdxRegex.FindStringSubmatch(line)
		var matchShuttingDown = shuttingDownRegex.FindString(line)
		var matchNewTurn = newTurnRegex.FindStringSubmatch(line)
		if matchLogIdx != nil {
			var newRaftLogReceived, _ = strconv.Atoi(matchLogIdx[1])
			if lastRaftLogReceived >= 0 && newRaftLogReceived != lastRaftLogReceived+1 {
				fmt.Printf("Node %d has a log gap: %d -> %d\n", node, lastRaftLogReceived, newRaftLogReceived)
			}
			lastRaftLogReceived = newRaftLogReceived
		}
		if matchSnapshotIdx != nil {
			var newRaftLogReceived, _ = strconv.Atoi(matchSnapshotIdx[1])
			// fmt.Printf("Node %d installed snapshot %d \n", node, newRaftLogReceived)
			lastRaftLogReceived = newRaftLogReceived
		}
		if matchNewTurn != nil {
			var newTurn, _ = strconv.Atoi(matchNewTurn[1])
			lastTurn = newTurn
		}
		if matchShuttingDown != "" {
			break
		}
	}
	if lastTurn >= 0 {
		fmt.Printf("Node %d: last turn %d\n", node, lastTurn)
	}
	fmt.Printf("Node %d: last received raft log %d\n", node, lastRaftLogReceived)
}

func readResults(node int) execStats {
	var regTs = regexp.MustCompile("[0-9]+")
	var regDropped = regexp.MustCompile("Action dropped")
	var stats = execStats{node, 0, 0, 0}
	file, _ := os.Open("/tmp/go_raft_" + fmt.Sprint(node))
	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	var fileTextLines []string

	for fileScanner.Scan() {
		fileTextLines = append(fileTextLines, fileScanner.Text())
	}

	file.Close()

	for _, eachline := range fileTextLines {
		matchTs := regTs.FindString(eachline)
		matchDropped := regDropped.FindString(eachline)
		if matchTs != "" {
			stats.nOfValues++
			var intVal, _ = strconv.Atoi(matchTs)
			stats.totValue += int64(intVal)
		}
		if matchDropped != "" {
			stats.dropped++
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
		os.Remove(f)
	}
}

func appendResultsToFile(filename string, text string) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(text + "\n"); err != nil {
		panic(err)
	}
}

func testNodes(pkgToTest string, number int, retChan chan bool) {

	removeAllLogFiles()
	fmt.Println("###################################")
	fmt.Println("##### Test for ", number, " nodes")
	// Start base nodes
	go baseNode(pkgToTest)
	time.Sleep(time.Millisecond * 1000)
	go newCommand(pkgToTest, "Node", 6667, 6666, 1)
	time.Sleep(time.Millisecond * 1000)
	go newCommand(pkgToTest, "Node", 6668, 6666, 2)
	time.Sleep(time.Millisecond * 1000)
	go newCommand(pkgToTest, "Node", 6669, 6666, 3)
	time.Sleep(time.Millisecond * 1000)
	go newCommand(pkgToTest, "Node", 6670, 6666, 4)
	time.Sleep(time.Millisecond * 1000)

	for i := 5; i < number; i++ {
		go newCommand(pkgToTest, "Bot", 6666+i, 6666+(i%5), i)
		time.Sleep(time.Millisecond * 1000)
	}
	time.Sleep(time.Second * timeToWait)
	killAll()

	var totalActionDelayMs float64 = 0
	var totalActionSent int64 = 0
	var totalDropped int64 = 0
	var nOfEntries int = 0
	var notStarted int = 0
	for i := 0; i < number; i++ {
		analyzeNodeBehavior(6666 + i)
		if i >= 5 {
			result := readResults(6666 + i)
			// fmt.Println(result)
			if result.nOfValues > 0 {
				var localMean = float64(result.totValue) / float64(result.nOfValues)
				totalDropped += result.dropped
				totalActionSent += result.nOfValues
				totalActionDelayMs += localMean
				nOfEntries++
			} else {
				notStarted++
			}
		}
	}
	if notStarted > 0 {
		fmt.Println("!!! Some nodes did not start: ", notStarted)
	}
	var resultAtionDelayMs = totalActionDelayMs / float64(nOfEntries)
	var resultDropped = float64(totalDropped) / float64(nOfEntries)
	var resultActionSent = float64(totalActionSent) / float64(nOfEntries)
	var actionsPerSecond = float64(resultActionSent) / float64(timeToWait)
	fmt.Println(fmt.Sprintf("Mean for %d nodes => actions: %.3f, actions per second: %.3f, delay: %.3f, dropped: %.3f", number, resultActionSent, actionsPerSecond, resultAtionDelayMs, resultDropped))
	appendResultsToFile("./results/"+pkgToTest, fmt.Sprintf("%d %.3f %.3f %.3f %.3f", number, resultActionSent, actionsPerSecond, resultAtionDelayMs, resultDropped))
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

func estimateTimeOfExecution(start int, stop int, step int) float64 {
	var result = 0.0
	for i := start; i <= stop; i += step {
		result += float64(i)
		result += timeToWait
		result += 10
	}
	return result
}

func main() {
	args := os.Args
	if len(args) < 5 {
		log.Fatal("Usage: go_test <go_skeletons | go_wanderer | both> <start> <finish> <step>")
	}
	var pkgToTest = args[1]
	var startStr = args[2]
	var stopStr = args[3]
	var stepStr = args[4]
	var start, _ = strconv.Atoi(startStr)
	var stop, _ = strconv.Atoi(stopStr)
	var step, _ = strconv.Atoi(stepStr)
	var both = false
	if pkgToTest == "both" {
		both = true
	}

	var estTimeOfExecution = estimateTimeOfExecution(start, stop, step)
	if both {
		estTimeOfExecution *= 2
	}
	fmt.Println(fmt.Sprintf("Est. time to wait: %.2f seconds (~%d minutes)", estTimeOfExecution, int(estTimeOfExecution)/60))

	if both {
		os.Remove("./results/go_skeletons")
		os.Remove("./results/go_wanderer")
	} else {
		os.Remove("./results/" + pkgToTest)
	}

	completeChan := make(chan bool)
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	go handlePrematureTermination(termChan, completeChan)

	if both {
		for i := start; i <= stop; i += step {
			retChan := make(chan bool)
			go testNodes("go_skeletons", i, retChan)
			<-retChan
			time.Sleep(time.Second * 10)
		}
		for i := start; i <= stop; i += step {
			retChan := make(chan bool)
			go testNodes("go_wanderer", i, retChan)
			<-retChan
			time.Sleep(time.Second * 10)
		}
	} else {
		for i := start; i <= stop; i += step {
			retChan := make(chan bool)
			go testNodes(pkgToTest, i, retChan)
			<-retChan
			time.Sleep(time.Second * 10)
		}
	}

	completeChan <- true
	time.Sleep(time.Second * 5)
	os.Exit(0)
}
