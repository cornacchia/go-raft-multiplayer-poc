#!/bin/bash

tc qdisc del dev lo root 2>/dev/null || true