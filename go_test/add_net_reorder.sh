#!/bin/bash

tc qdisc add dev lo root netem delay 10ms reorder 25% 50%