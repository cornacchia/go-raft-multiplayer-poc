#!/bin/bash

tc qdisc add dev lo root netem delay 20 10ms 25%