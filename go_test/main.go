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

	log "github.com/sirupsen/logrus"
)

type execStats struct {
	node            int
	totValue        int64
	nOfValues       int64
	dropped         int64
	startTs         *time.Time
	endTs           *time.Time
	durationSeconds float64
	lastRaftLog     int
}

type results struct {
	nodes            int
	actionsSent      float64
	actionsPerSecond float64
	actionDelay      float64
	droppedActions   float64
	unconnectedNodes float64
}

var openCmds = [1024]*exec.Cmd{}

func newCommand(pkgToTest string, mode string, ports []string, idx int, connectMode string) {
	var cmd *exec.Cmd
	var cmdString = "./" + pkgToTest
	var args = []string{"Test", mode, connectMode}
	cmd = exec.Command(cmdString, append(args, ports...)...)
	cmd.Dir = "../" + pkgToTest
	cmd.Stderr = os.Stderr
	openCmds[idx] = cmd
	cmd.Start()
	if err := cmd.Wait(); err != nil {
		log.Trace("Error starting command ", cmd.Path, " ", cmd.Args)
		log.Trace(err)
	}
}

func killProcess(i int, signal syscall.Signal) {
	if openCmds[i] != nil {
		(*openCmds[i]).Process.Signal(signal)
		(*openCmds[i]).Wait()
	}
}

func killAll(pkgToTest string) {
	cmd := exec.Command("killall", "-SIGKILL", pkgToTest)
	log.Trace("Killing all workers")
	err := cmd.Run()
	log.Trace("Killing finished with error: ", err)
}

func analyzeNodeBehavior(node int) execStats {
	log.Debug("###################### ", node, " #####################")
	const timestampLayout = "2006-01-02 15:04:05.000000"
	var lastRaftLogReceived = -1
	var lastTurn = -1
	var newTurnRegex = regexp.MustCompile("New turn: ([0-9]+)")
	var raftLogIdxRegex = regexp.MustCompile("Raft apply log: ([0-9]+)")
	var stateSnapshotIdxRegex = regexp.MustCompile("State: install snapshot ([0-9]+)")
	var regTs = regexp.MustCompile("Main - Action time: ([0-9]+)")
	var regDropped = regexp.MustCompile("Main - Action dropped - ")
	var regTimestamp = regexp.MustCompile("time=\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}\\d{3})\"")
	var stats = execStats{node, 0, 0, 0, nil, nil, -1, 0}

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
				log.Debug(fmt.Sprintf("Node %d has a log gap: %d -> %d\n", node, lastRaftLogReceived, newRaftLogReceived))
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
		log.Debug("Last turn: ", lastTurn)
	}
	log.Debug("N. of actions: ", stats.nOfValues)
	log.Debug("Last received raft log: ", lastRaftLogReceived)
	stats.lastRaftLog = lastRaftLogReceived
	stats.durationSeconds = (*stats.endTs).Sub((*stats.startTs)).Seconds()
	return stats
}

func appendResultsToFile(filename string, res results) {
	var text = fmt.Sprintf("%d %.3f %.3f %.3f %.3f %.3f", res.nodes, res.actionsSent, res.actionsPerSecond, res.actionDelay, res.droppedActions, res.unconnectedNodes)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(text + "\n"); err != nil {
		panic(err)
	}
}

func elaborateResults(start int, clientStart int, stop int, pkgToTest string, testTime int, testMode string) results {
	var totalActionDelayMs float64 = 0
	var totalActionSent int64 = 0
	var totalDropped int64 = 0
	var totalActionsPerSecond = 0.0
	var nOfEntries int = 0
	var notStarted int = 0
	var unconnectedNodes float64 = 0
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
			if result.lastRaftLog < 0 {
				unconnectedNodes += 1.0
			}
		}
	}
	if notStarted > 0 {
		log.Info("!!! Some nodes did not start: ", notStarted)
	}
	var resultActionDelayMs = totalActionDelayMs / float64(nOfEntries)
	var resultDropped = float64(totalDropped) / float64(nOfEntries)
	var resultActionSent = float64(totalActionSent) / float64(nOfEntries)
	var actionsPerSecond = totalActionsPerSecond / float64(nOfEntries)
	log.Info(fmt.Sprintf("Mean for %d nodes => actions: %.3f, actions per second: %.3f, delay: %.3f, dropped: %.3f, unconnected: %.3f", stop, resultActionSent, actionsPerSecond, resultActionDelayMs, resultDropped, unconnectedNodes))
	return results{stop, resultActionSent, actionsPerSecond, resultActionDelayMs, resultDropped, unconnectedNodes}
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

func testNodesNormal(testMode string, pkgToTest string, number int, testTime int, retChan chan results) {
	removeAllLogFiles()
	log.Debug("###################################")
	log.Debug("##### Normal test for ", number, " nodes, ")

	var nodesToTest = map[int][]string{
		0: {"6666", "6667", "6668", "6669", "6670"},
		1: {"6667", "6666", "6668", "6669", "6670"},
		2: {"6668", "6666", "6667", "6669", "6670"},
		3: {"6669", "6666", "6667", "6668", "6670"},
		4: {"6670", "6666", "6667", "6668", "6669"},
	}

	var clientsToTest = map[int][]string{}

	for i := 0; i < number; i++ {
		clientsToTest[i] = []string{fmt.Sprint(6666 + i + 5), "6666", "6667", "6668", "6669", "6670"}
	}

	for i, nodes := range nodesToTest {
		go newCommand(pkgToTest, "Node", nodes, i, "Full")
		time.Sleep(time.Millisecond * 1000)
	}

	for i, nodes := range clientsToTest {
		go newCommand(pkgToTest, "Bot", nodes, i+5, "Append")
		time.Sleep(time.Millisecond * 1000)
	}

	waitRetChan := make(chan bool)
	go waitSomeTime(time.Duration(testTime), []chan bool{waitRetChan})
	<-waitRetChan

	killAll(pkgToTest)

	retChan <- elaborateResults(0, 5, len(nodesToTest)+len(clientsToTest), pkgToTest, testTime, testMode)
}

func testNormal(testMode string, start int, stop int, step int, testTime int, repetitions int, pkgToTest string, resultFile string) {
	for i := start; i <= stop; i += step {
		var gr = results{i, 0.0, 0.0, 0.0, 0.0, 0.0}
		retChan := make(chan results)
		for j := 0; j < repetitions; j++ {
			go testNodesNormal(testMode, pkgToTest, i, testTime, retChan)
			cr := <-retChan
			gr.actionsSent += cr.actionsSent
			gr.actionsPerSecond += cr.actionsPerSecond
			gr.actionDelay += cr.actionDelay
			gr.droppedActions += cr.droppedActions
			gr.unconnectedNodes += cr.unconnectedNodes
			time.Sleep(time.Second * 10)
		}
		gr.actionsSent = gr.actionsSent / float64(repetitions)
		gr.actionsPerSecond = gr.actionsPerSecond / float64(repetitions)
		gr.actionDelay = gr.actionDelay / float64(repetitions)
		gr.droppedActions = gr.droppedActions / float64(repetitions)
		gr.unconnectedNodes = gr.unconnectedNodes / float64(repetitions)
		log.Info(fmt.Sprintf("Mean results for %d nodes and %d repetitions => actions: %.3f, actions per second: %.3f, delay: %.3f, dropped: %.3f, unconnected: %.3f", i, repetitions, gr.actionsSent, gr.actionsPerSecond, gr.actionDelay, gr.droppedActions, gr.unconnectedNodes))
		if resultFile != "" {
			appendResultsToFile("./results/"+resultFile, gr)
		}
	}
}

func killWorkers(pkgToTest string, signal syscall.Signal, killInterval int, retChan chan bool, nodesToTest map[int][]string) {
	var currentWorkerIdx = 0
	log.Debug("Killing nodes: ")
	for {
		select {
		case <-retChan:
			log.Debug("Stop killing nodes")
			return
		case <-time.After(time.Second * time.Duration(killInterval)):
			killProcess(currentWorkerIdx, signal)
			time.Sleep(time.Second * 10)
			var val, _ = nodesToTest[currentWorkerIdx]
			go newCommand(pkgToTest, "Node", val, currentWorkerIdx, "Append")
			currentWorkerIdx = (currentWorkerIdx + 1) % len(nodesToTest)
		}
	}
}

func testNodesDynamic(testMode string, killInterval int, testTime int, pkgToTest string, retChan chan results, signal syscall.Signal) {
	removeAllLogFiles()
	log.Debug("###################################")
	log.Debug("##### Dynamic test for nodes: ")
	if signal == syscall.SIGTERM {
		log.Debug("SIGTERM")
	} else {
		log.Debug("SIGKILL")
	}

	var nodesToTest = map[int][]string{
		0: {"6666", "6667", "6668", "6669", "6670"},
		1: {"6667", "6666", "6668", "6669", "6670"},
		2: {"6668", "6666", "6667", "6669", "6670"},
		3: {"6669", "6666", "6667", "6668", "6670"},
		4: {"6670", "6666", "6667", "6668", "6669"},
	}

	var clientsToTest = map[int][]string{
		5: {"6671", "6666", "6667", "6668", "6669", "6670"},
		6: {"6672", "6666", "6667", "6668", "6669", "6670"},
		7: {"6673", "6666", "6667", "6668", "6669", "6670"},
		8: {"6674", "6666", "6667", "6668", "6669", "6670"},
		9: {"6675", "6666", "6667", "6668", "6669", "6670"},
	}

	for i, nodes := range nodesToTest {
		go newCommand(pkgToTest, "Node", nodes, i, "Full")
		time.Sleep(time.Millisecond * 10)
	}

	for i, nodes := range clientsToTest {
		go newCommand(pkgToTest, "Bot", nodes, i+5, "Append")
		time.Sleep(time.Millisecond * 10)
	}

	waitRetChan := make(chan bool)
	stopKillingWorkersChan := make(chan bool)
	go waitSomeTime(time.Duration(testTime), []chan bool{stopKillingWorkersChan, waitRetChan})
	go killWorkers(pkgToTest, signal, killInterval, stopKillingWorkersChan, nodesToTest)
	<-waitRetChan

	killAll(pkgToTest)

	retChan <- elaborateResults(0, 5, len(nodesToTest)+len(clientsToTest), pkgToTest, testTime, testMode)
}

func testDynamic(testMode string, start int, stop int, step int, testTime int, repetitions int, pkgToTest string, resultFile string, signal syscall.Signal) {
	for i := start; i <= stop; i += step {
		var gr = results{i, 0.0, 0.0, 0.0, 0.0, 0.0}
		retChan := make(chan results)
		for j := 0; j < repetitions; j++ {
			go testNodesDynamic(testMode, i, testTime, pkgToTest, retChan, signal)
			cr := <-retChan
			gr.actionsSent += cr.actionsSent
			gr.actionsPerSecond += cr.actionsPerSecond
			gr.actionDelay += cr.actionDelay
			gr.droppedActions += cr.droppedActions
			time.Sleep(time.Second * 10)
		}
		gr.actionsSent = gr.actionsSent / float64(repetitions)
		gr.actionsPerSecond = gr.actionsPerSecond / float64(repetitions)
		gr.actionDelay = gr.actionDelay / float64(repetitions)
		gr.droppedActions = gr.droppedActions / float64(repetitions)
		gr.unconnectedNodes = gr.unconnectedNodes / float64(repetitions)
		log.Info(fmt.Sprintf("Mean results for %d nodes and %d repetitions => actions: %.3f, actions per second: %.3f, delay: %.3f, dropped: %.3f, unconnected: %.3f", i, repetitions, gr.actionsSent, gr.actionsPerSecond, gr.actionDelay, gr.droppedActions, gr.unconnectedNodes))

		if resultFile != "" {
			appendResultsToFile("./results/"+resultFile, gr)
		}
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

func estimateTimeOfExecution(start int, stop int, step int, testTime int, repetitions int, mode string) float64 {
	var result = 0.0
	for i := start; i <= stop; i += step {
		for j := 0; j < repetitions; j++ {
			if mode == "normal" {
				result += float64(i)
			} else {
				result += 5
			}
			result += float64(testTime)
			result += 10
		}
	}
	result += 5
	return result
}

func testPackage(testMode string, pkgToTest string, start int, stop int, step int, testTime int, repetitions int, resultFile string) {
	if resultFile != "" {
		os.Remove("./results/" + resultFile)
	}

	completeChan := make(chan bool)
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	go handlePrematureTermination(pkgToTest, termChan, completeChan)

	if testMode == "normal" {
		testNormal(testMode, start, stop, step, testTime, repetitions, pkgToTest, resultFile)
	} else if testMode == "dynamic" {
		testDynamic(testMode, start, stop, step, testTime, repetitions, pkgToTest, resultFile, syscall.SIGTERM)
	} else if testMode == "faulty" {
		testDynamic(testMode, start, stop, step, testTime, repetitions, pkgToTest, resultFile, syscall.SIGKILL)
	}

	completeChan <- true
	time.Sleep(time.Second * 5)
}

func main() {
	log.SetLevel(log.InfoLevel)
	args := os.Args
	if len(args) < 8 {
		log.Fatal("Usage: go_test <dynamic | faulty | normal> <go_skeletons | go_wanderer | both> <repetitions> <test time> <start> <finish> <step> <result_file>")
	}
	var testMode = args[1]
	var pkgToTest = args[2]
	var repetitionsStr = args[3]
	var testTimeStr = args[4]
	var testTime, _ = strconv.Atoi(testTimeStr)
	var repetitions, _ = strconv.Atoi(repetitionsStr)
	var start = -1
	var stop = -1
	var step = -1
	var startStr = args[5]
	var stopStr = args[6]
	var stepStr = args[7]
	start, _ = strconv.Atoi(startStr)
	stop, _ = strconv.Atoi(stopStr)
	step, _ = strconv.Atoi(stepStr)
	var resultFile = ""
	if len(args) > 8 {
		resultFile = args[8]
	}

	var estTimeOfExecution = estimateTimeOfExecution(start, stop, step, testTime, repetitions, testMode)
	if pkgToTest == "both" {
		estTimeOfExecution *= 2
	}
	log.Info(fmt.Sprintf("Est. time to wait: %.2f seconds (~%d minutes)", estTimeOfExecution, int(estTimeOfExecution)/60))

	if pkgToTest == "both" {
		testPackage(testMode, "go_skeletons", start, stop, step, testTime, repetitions, resultFile)
		testPackage(testMode, "go_wanderer", start, stop, step, testTime, repetitions, resultFile)
	} else {
		testPackage(testMode, pkgToTest, start, stop, step, testTime, repetitions, resultFile)
	}
	os.Exit(0)
}
