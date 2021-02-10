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
	var cmd *exec.Cmd
	if startLeaderPort >= 0 {
		cmd = exec.Command("./"+pkgToTest, "Test", mode, fmt.Sprint(port), fmt.Sprint(startLeaderPort))
	} else {
		cmd = exec.Command("./"+pkgToTest, "Test", mode, fmt.Sprint(port))
	}
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

func killProcess(i int, signal syscall.Signal) {
	if openCmds[i] != nil {
		(*openCmds[i]).Process.Signal(signal)
		(*openCmds[i]).Wait()
	}
}

func killAll() {
	fmt.Println("Killall: ", numberOfCmds)
	for idx := 0; idx < numberOfCmds; idx++ {
		killProcess(idx, syscall.SIGTERM)
	}
}

func analyzeNodeBehavior(node int) {
	fmt.Printf("###################### %d #####################\n", node)
	var lastRaftLogReceived = -1
	var lastTurn = -1
	var newTurnRegex = regexp.MustCompile("New turn: ([0-9]+)")
	var raftLogIdxRegex = regexp.MustCompile("Raft apply log: ([0-9]+)")
	var stateSnapshotIdxRegex = regexp.MustCompile("State: install snapshot ([0-9]+)")

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

func elaborateResults(start int, clientStart int, stop int, pkgToTest string, testTime int, testMode string) {
	var totalActionDelayMs float64 = 0
	var totalActionSent int64 = 0
	var totalDropped int64 = 0
	var nOfEntries int = 0
	var notStarted int = 0
	for i := start; i < stop; i++ {
		analyzeNodeBehavior(6666 + i)
		if i >= clientStart {
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
	var actionsPerSecond = float64(resultActionSent) / float64(testTime)
	fmt.Println(fmt.Sprintf("Mean for %d nodes => actions: %.3f, actions per second: %.3f, delay: %.3f, dropped: %.3f", stop, resultActionSent, actionsPerSecond, resultAtionDelayMs, resultDropped))
	appendResultsToFile("./results/"+pkgToTest+"_"+testMode, fmt.Sprintf("%d %.3f %.3f %.3f %.3f", stop, resultActionSent, actionsPerSecond, resultAtionDelayMs, resultDropped))
}

func waitSomeTime(duration time.Duration, retChans []chan bool) {
	time.Sleep(time.Second * duration)
	for _, ch := range retChans {
		ch <- true
	}
}

func testNodes(testMode string, pkgToTest string, number int, testTime int, retChan chan bool) {
	removeAllLogFiles()
	fmt.Println("###################################")
	fmt.Println("##### Normal test for ", number, " nodes")
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

	waitRetChan := make(chan bool)
	go waitSomeTime(time.Duration(testTime), []chan bool{waitRetChan})
	<-waitRetChan
	killAll()

	elaborateResults(0, 5, number, pkgToTest, testTime, testMode)

	retChan <- true
}

func testNormal(testMode string, start int, stop int, step int, testTime int, pkgToTest string) {
	for i := start; i <= stop; i += step {
		retChan := make(chan bool)
		go testNodes(testMode, pkgToTest, i, testTime, retChan)
		<-retChan
		time.Sleep(time.Second * 10)
	}
}

func killWorkers(pkgToTest string, signal syscall.Signal, retChan chan bool, nodesToTest map[int][2]int) {
	var currentWorkerIdx = 0
	for {
		select {
		case <-retChan:
			return
		case <-time.After(time.Second * 20):
			killProcess(currentWorkerIdx, signal)
			time.Sleep(time.Second * 5)
			var val, _ = nodesToTest[currentWorkerIdx]
			go newCommand(pkgToTest, "Bot", val[0], val[1], currentWorkerIdx)
			currentWorkerIdx = (currentWorkerIdx + 1) % len(nodesToTest)
		}
	}
}

func testDynamic(testMode string, testTime int, pkgToTest string, signal syscall.Signal) {
	removeAllLogFiles()
	fmt.Println("###################################")
	fmt.Print("##### Dynamic test for nodes: ")
	if signal == syscall.SIGTERM {
		fmt.Println("SIGTERM")
	} else {
		fmt.Println("SIGKILL")
	}

	var nodesToTest = map[int][2]int{
		1: {6667, 6666},
		2: {6668, 6666},
		3: {6669, 6666},
		4: {6670, 6666},
	}

	go newCommand(pkgToTest, "Bot", 6666, -1, 0)
	time.Sleep(time.Millisecond * 200)
	for i, nodes := range nodesToTest {
		go newCommand(pkgToTest, "Bot", nodes[0], nodes[1], i)
		time.Sleep(time.Millisecond * 200)
	}
	nodesToTest[0] = [2]int{6666, 6667}

	waitRetChan := make(chan bool)
	stopKillingWorkersChan := make(chan bool)
	go waitSomeTime(time.Duration(testTime), []chan bool{stopKillingWorkersChan, waitRetChan})
	go killWorkers(pkgToTest, signal, stopKillingWorkersChan, nodesToTest)
	<-waitRetChan

	time.Sleep(time.Second * time.Duration(testTime))
	killAll()

	elaborateResults(0, 0, len(nodesToTest), pkgToTest, testTime, testMode)
}

func handlePrematureTermination(termChan chan os.Signal, completeChan chan bool) {
	select {
	case <-termChan:
		killAll()
		os.Exit(0)
	case <-completeChan:
	}
}

func estimateTimeOfExecution(start int, stop int, step int, testTime int) float64 {
	var result = 0.0
	for i := start; i <= stop; i += step {
		result += float64(i)
		result += float64(testTime)
		result += 10
	}
	return result
}

func main() {
	args := os.Args
	if len(args) < 4 {
		log.Fatal("Usage: go_test <dynamic | faulty | normal> <go_skeletons | go_wanderer> <time> [<start> <finish> <step>]")
	}
	var testMode = args[1]
	var pkgToTest = args[2]
	var testTimeStr = args[3]
	var testTime, _ = strconv.Atoi(testTimeStr)
	var start = -1
	var stop = -1
	var step = -1
	if len(args) > 4 {
		var startStr = args[4]
		var stopStr = args[5]
		var stepStr = args[6]
		start, _ = strconv.Atoi(startStr)
		stop, _ = strconv.Atoi(stopStr)
		step, _ = strconv.Atoi(stepStr)
	}

	if testMode == "normal" {
		var estTimeOfExecution = estimateTimeOfExecution(start, stop, step, testTime)
		fmt.Println(fmt.Sprintf("Est. time to wait: %.2f seconds (~%d minutes)", estTimeOfExecution, int(estTimeOfExecution)/60))
	}

	os.Remove("./results/" + pkgToTest + "_" + testMode)

	completeChan := make(chan bool)
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	go handlePrematureTermination(termChan, completeChan)

	if testMode == "normal" {
		testNormal(testMode, start, stop, step, testTime, pkgToTest)
	} else if testMode == "dynamic" {
		testDynamic(testMode, testTime, pkgToTest, syscall.SIGTERM)
	} else if testMode == "faulty" {
		testDynamic(testMode, testTime, pkgToTest, syscall.SIGKILL)
	}

	completeChan <- true
	time.Sleep(time.Second * 5)
	os.Exit(0)
}
