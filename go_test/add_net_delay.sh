#!/bin/bash

tc qdisc add dev lo root netem delay 40 10ms 25%