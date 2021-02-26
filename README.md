# Multiplayer POC based on Raft

![](images/go_skeletons.png)

This repository holds an implementation of the [Raft consensus algorithm](https://raft.github.io/) written in [Go](https://golang.org/) along with two (very) simple games (basically two movement demos, there's no real gameplay to speak of at this point) which were used to test the possibility of running online multiplayer video games on a distributed state machine.

* **go_skeletons**: The game shown in the figure above, basically a [2.5D engine](https://en.wikipedia.org/wiki/2.5D) with little graphics on top (skeleton sprites were made by Jacopo F. for our game [42 vs. Evil](https://cornacchia.itch.io/42-vs-evil), the main logic of the game engine is based on great tutorials by [OneLoneCoder](https://github.com/OneLoneCoder), specifically [this](https://github.com/OneLoneCoder/CommandLineFPS/blob/master/CommandLineFPS.cpp) and [this](https://github.com/OneLoneCoder/videos/blob/master/OneLoneCoder_ComandLineFPS_2.cpp)). This "game" was created to test fast-paced real time games.
* **go_wanderer**: A grid based game in which a player simply walk around their red square. Fully turn-based (i.e. every player must make a move before a new one becomes available) and, indeed, created only to test this scenario.

## Usage

The Raft core logic is in the module `go_raft` which is imported by the two games. To build the games:

* `$ cd go_skeletons`
* `$ go build`
* `$ cd ../go_wanderer`
* `$ go build`

### Running on branch main

In branch `main` clients processes always run a Raft node as well, so to test a game with two players on localhost open two terminals from the `go_skeletons` folder and run `./go_skeletons Game Client Append 6666` in the first and `./go_skeletons Game Client Append 6667 6666` in the second.

Both games are controlled with the standard WASD keys.

### Running on branch server-cluster

In branch `server-cluster` Raft nodes and clients are always separated, to run a game with two players first a Raft cluster must be started e.g. with `./go_skeletons Game Node Full 6666 6667` and `./go_skeletons Game Node Full 6667 6666` and then clients can be attached to the network with `./go_skeletons Game Client 6668 6666 66667`, `./go_skeletons Game Client 6669 6666 66667`, and so on.

Both games are controlled with the standard WASD keys.

### Running on branch byzantine-behavior

Branch `byzantine-behavior` is an attempt to reproduce what is described in [this paper](https://www.scs.stanford.edu/14au-cs244b/labs/projects/copeland_zhong.pdf). It works mostly like the `main` branch, but nodes (and clients) should always be started with the `Full` option as this version is not properly adapted to configuration changes.
Moreover nodes and clients need different public and private keys (i.e. a single process launched in `Game` mode will a pair of keys for both its node and client), these should be saved in `keys/clients` and `keys/nodes`. Public keys should be called `public_key_<node port>.pem`, private keys shoudl be called `key_<node port>.pem`.
The `go_test` script will handle key creation itself.

## Rundown of command-line options

The available options are the following:

`go_skeletons <Game|Test> <Node|Client|Bot> <Append|Full> port otherPort1 otherPort2 ...`

* `Game` mode is used to actually run the games in the terminal and see the UI and logs, `Test` mode is used to run the tests, logs will be saved to file and there will be no UI.
* `Node` mode runs only the Raft logic, `Client` mode runs both node and game client in branch `main` but only the game client in `server-cluster`, `Bot` mode will create a player which will perform random actions.
* `Append` mode is used to attach new nodes to an already-existing Raft network, a new node will issue a configuration change request to the network and wait until fully connected before fully initializing its logic; `Full` mode is used to start a Raft network in which every participant knows the others beforehand, this is especially useful when starting a cluster of nodes in `server-cluster`.
* `port` is the port used by the current process.
* `otherPort1, ...` are the nodes the current process should be aware of when starting.

## Raft analyzer

The directory `raft-analyzer` contains a simple nodejs server built with [sails](https://sailsjs.com/) and can be used to analyze and graphically follow traces of Raft communication (generated in test mode).

* It was tested with nodejs version 14.15.4.
* It requires a mongodb database, it will use `mongodb://localhost:27017/` by default but can be configured in `raft-analyzer/config/datastores.js`

To start it `cd` to the raft-analyzer directory, run `npm install`, then `npm start` and the interface will be up on `http://localhost:1337`.

## Running some tests

The folder `go_test` holds a script which can be used to generate some Raft communication traces. To run tests `cd` to the `go_test` directory:

`go run main.go <normal|dynamic|faulty> <go_skeletons|go_wanderer> repetitions time start end step`

All tests will start a number of nodes and clients in bot mode:
* `normal` is a normal execution with no surprises, `dynamic` will kill nodes with SIGTERM (will cause a configuration change), `faulty` will kill nodes with SIGKILL (note in these two modes there will always be 5 nodes and 5 clients)
* `go_skeletons` and `go_wanderer` will, unsurprisingly, select one of the two games for the test
* `repetitions` is an integer value which can be used to collect median results on several runs of the test
* `time` an integer which determines how many seconds a test will run
* `start` for `normal` mode this indicates how many game clients it will start with, for `dynamic` and `faulty` mode it will determine the starting kill frequency
* `end` same as above but for the final number of nodes/kill frequency
* `step` used to increase the value from `start` to `end`

There are many different options so here are some examples:

* To test groups of clients of 5, 10, 15 for 1 minute each in normal mode: `go run main.go normal go_skeletons 1 60 5 15 5`
* To test a kill frequency of ~30 seconds for two minutes and collect the results of 5 subsequent runs: `go run main.go dynamic go_skeletons 5 120 30 30 1`

Test logs will be saved in `/tmp` and be named `go_raft_log_<port>` and can be loaded in `raft-analyzer`, however this should only be done for small clusters (5-10) and small run times (~1 minute) because the analyzer is not particularly optimized at this stage.