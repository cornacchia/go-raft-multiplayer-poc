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
	node            int
	totValue        int64
	nOfValues       int64
	dropped         int64
	startTs         *time.Time
	endTs           *time.Time
	durationSeconds float64
}

var openCmds = [1024]*exec.Cmd{}

func newCommand(pkgToTest string, mode string, ports []string, idx int) {
	var cmd *exec.Cmd
	var cmdString = "./" + pkgToTest
	var args = []string{"Test", mode}
	cmd = exec.Command(cmdString, append(args, ports...)...)
	cmd.Dir = "../" + pkgToTest
	cmd.Stderr = os.Stderr
	openCmds[idx] = cmd
	cmd.Start()
	if err := cmd.Wait(); err != nil {
		fmt.Println("Error starting command ", cmd.Path, " ", cmd.Args)
		fmt.Println(err)
	}
}

func killProcess(i int, signal syscall.Signal) {
	if openCmds[i] != nil {
		(*openCmds[i]).Process.Signal(signal)
		(*openCmds[i]).Wait()
	}
}

func killAll(pkgToTest string) {
	cmd := exec.Command("killall", "-SIGTERM", pkgToTest)
	log.Printf("Killing all workers")
	err := cmd.Run()
	log.Printf("Killing finished with error: %v", err)
}

func analyzeNodeBehavior(node int) execStats {
	fmt.Printf("###################### %d #####################\n", node)
	const timestampLayout = "2006-01-02 15:04:05.000000"
	var lastRaftLogReceived = -1
	var lastTurn = -1
	var newTurnRegex = regexp.MustCompile("New turn: ([0-9]+)")
	var raftLogIdxRegex = regexp.MustCompile("Raft apply log: ([0-9]+)")
	var stateSnapshotIdxRegex = regexp.MustCompile("State: install snapshot ([0-9]+)")
	var regTs = regexp.MustCompile("Main - Action time: ([0-9]+)")
	var regDropped = regexp.MustCompile("Main - Action dropped - ")
	var regTimestamp = regexp.MustCompile("time=\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}\\d{3})\"")
	var stats = execStats{node, 0, 0, 0, nil, nil, -1}

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
		var matchTs = regTs.FindStringSubmatch(line)
		var matchDropped = regDropped.FindStringSubmatch(line)
		var matchTimestamp = regTimestamp.FindStringSubmatch(line)

		if matchTimestamp != nil {
			var timeString = matchTimestamp[1]
			var time, _ = time.Parse(timestampLayout, timeString)
			if stats.startTs == nil {
				stats.startTs = &time
			}
			stats.endTs = &time
		}
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
		if matchTs != nil {
			stats.nOfValues++
			var intVal, _ = strconv.Atoi(matchTs[1])
			stats.totValue += int64(intVal)
		}
		if matchDropped != nil {
			stats.dropped++
		}
	}
	if lastTurn >= 0 {
		fmt.Printf("Node %d: last turn %d\n", node, lastTurn)
	}
	fmt.Printf("Node %d: last received raft log %d\n", node, lastRaftLogReceived)
	stats.durationSeconds = (*stats.endTs).Sub((*stats.startTs)).Seconds()
	return stats
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
	var totalActionsPerSecond = 0.0
	var nOfEntries int = 0
	var notStarted int = 0
	for i := start; i < stop; i++ {
		result := analyzeNodeBehavior(6666 + i)
		if i >= clientStart {
			if result.nOfValues > 0 {
				var localMean = float64(result.totValue) / float64(result.nOfValues)
				var localActionsPerSecond = float64(result.nOfValues) / result.durationSeconds
				totalActionsPerSecond += localActionsPerSecond
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
	var resultActionDelayMs = totalActionDelayMs / float64(nOfEntries)
	var resultDropped = float64(totalDropped) / float64(nOfEntries)
	var resultActionSent = float64(totalActionSent) / float64(nOfEntries)
	var actionsPerSecond = totalActionsPerSecond / float64(nOfEntries)
	fmt.Println(fmt.Sprintf("Mean for %d nodes => actions: %.3f, actions per second: %.3f, delay: %.3f, dropped: %.3f", stop, resultActionSent, actionsPerSecond, resultActionDelayMs, resultDropped))
	appendResultsToFile("./results/"+pkgToTest+"_"+testMode, fmt.Sprintf("%d %.3f %.3f %.3f %.3f", stop, resultActionSent, actionsPerSecond, resultActionDelayMs, resultDropped))
}

func waitSomeTime(duration time.Duration, retChans []chan bool) {
	time.Sleep(time.Second * duration)
	for _, ch := range retChans {
		ch <- true
	}
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

func testNodesNormal(testMode string, pkgToTest string, number int, testTime int, retChan chan bool) {
	removeAllLogFiles()
	fmt.Println("###################################")
	fmt.Println("##### Normal test for ", number, " nodes")

	var nodesToTest = map[int][]string{
		1: {"6667", "6666"},
		2: {"6668", "6666"},
		3: {"6669", "6666"},
		4: {"6670", "6666"},
	}

	for i := 5; i < number; i++ {
		// nodesToTest[i] = []string{fmt.Sprint(6666 + i), fmt.Sprint(6666 + (i % 5))}
		nodesToTest[i] = []string{fmt.Sprint(6666 + i), "6666"}
	}

	go newCommand(pkgToTest, "Bot", []string{"6666"}, 0)
	time.Sleep(time.Millisecond * 1000)
	for i, nodes := range nodesToTest {
		go newCommand(pkgToTest, "Bot", nodes, i)
		time.Sleep(time.Millisecond * 1000)
	}

	waitRetChan := make(chan bool)
	go waitSomeTime(time.Duration(testTime), []chan bool{waitRetChan})
	<-waitRetChan

	killAll(pkgToTest)

	elaborateResults(0, 0, number, pkgToTest, testTime, testMode)

	retChan <- true
}

func testNormal(testMode string, start int, stop int, step int, testTime int, pkgToTest string) {
	for i := start; i <= stop; i += step {
		retChan := make(chan bool)
		go testNodesNormal(testMode, pkgToTest, i, testTime, retChan)
		<-retChan
		time.Sleep(time.Second * 10)
	}
}

func killWorkers(pkgToTest string, signal syscall.Signal, killInterval int, retChan chan bool, nodesToTest map[int][]string) {
	var currentWorkerIdx = 0
	fmt.Print("Killing nodes: ")
	for {
		select {
		case <-retChan:
			fmt.Println("Stop killing nodes")
			return
		case <-time.After(time.Second * time.Duration(killInterval)):
			fmt.Print(currentWorkerIdx, ", ")
			killProcess(currentWorkerIdx, signal)
			time.Sleep(time.Second * 10)
			var val, _ = nodesToTest[currentWorkerIdx]
			go newCommand(pkgToTest, "Bot", val, currentWorkerIdx)
			currentWorkerIdx = (currentWorkerIdx + 1) % len(nodesToTest)
		}
	}
}

func testNodesDynamic(testMode string, killInterval int, testTime int, pkgToTest string, retChan chan bool, signal syscall.Signal) {
	removeAllLogFiles()
	fmt.Println("###################################")
	fmt.Print("##### Dynamic test for nodes: ")
	if signal == syscall.SIGTERM {
		fmt.Println("SIGTERM")
	} else {
		fmt.Println("SIGKILL")
	}

	var nodesToTest = map[int][]string{
		1: {"6667", "6666", "6668"},
		2: {"6668", "6666", "6669"},
		3: {"6669", "6666", "6670"},
		4: {"6670", "6666", "6667"},
	}

	go newCommand(pkgToTest, "Bot", []string{"6666"}, 0)
	time.Sleep(time.Millisecond * 1000)
	for i, nodes := range nodesToTest {
		go newCommand(pkgToTest, "Bot", nodes, i)
		time.Sleep(time.Millisecond * 1000)
	}
	nodesToTest[0] = []string{"6666", "6667", "6668"}

	waitRetChan := make(chan bool)
	stopKillingWorkersChan := make(chan bool)
	go waitSomeTime(time.Duration(testTime), []chan bool{stopKillingWorkersChan, waitRetChan})
	go killWorkers(pkgToTest, signal, killInterval, stopKillingWorkersChan, nodesToTest)
	<-waitRetChan

	killAll(pkgToTest)

	elaborateResults(0, 0, len(nodesToTest), pkgToTest, testTime, testMode)
	retChan <- true
}

func testDynamic(testMode string, start int, stop int, step int, testTime int, pkgToTest string, signal syscall.Signal) {
	for i := start; i <= stop; i += step {
		retChan := make(chan bool)
		go testNodesDynamic(testMode, i, testTime, pkgToTest, retChan, signal)
		<-retChan
		time.Sleep(time.Second * 10)
	}
}

func handlePrematureTermination(pkgToTest string, termChan chan os.Signal, completeChan chan bool) {
	select {
	case <-termChan:
		killAll(pkgToTest)
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
	if len(args) < 7 {
		log.Fatal("Usage: go_test <dynamic | faulty | normal> <go_skeletons | go_wanderer> <time> <start> <finish> <step>")
	}
	var testMode = args[1]
	var pkgToTest = args[2]
	var testTimeStr = args[3]
	var testTime, _ = strconv.Atoi(testTimeStr)
	var start = -1
	var stop = -1
	var step = -1
	var startStr = args[4]
	var stopStr = args[5]
	var stepStr = args[6]
	start, _ = strconv.Atoi(startStr)
	stop, _ = strconv.Atoi(stopStr)
	step, _ = strconv.Atoi(stepStr)

	if testMode == "normal" {
		var estTimeOfExecution = estimateTimeOfExecution(start, stop, step, testTime)
		fmt.Println(fmt.Sprintf("Est. time to wait: %.2f seconds (~%d minutes)", estTimeOfExecution, int(estTimeOfExecution)/60))
	}

	os.Remove("./results/" + pkgToTest + "_" + testMode)

	completeChan := make(chan bool)
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	go handlePrematureTermination(pkgToTest, termChan, completeChan)

	if testMode == "normal" {
		testNormal(testMode, start, stop, step, testTime, pkgToTest)
	} else if testMode == "dynamic" {
		testDynamic(testMode, start, stop, step, testTime, pkgToTest, syscall.SIGTERM)
	} else if testMode == "faulty" {
		testDynamic(testMode, start, stop, step, testTime, pkgToTest, syscall.SIGKILL)
	}

	completeChan <- true
	time.Sleep(time.Second * 5)
	os.Exit(0)
}
