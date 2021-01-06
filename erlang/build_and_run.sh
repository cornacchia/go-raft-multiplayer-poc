#!/bin/bash
erlc state.erl
erlc node.erl
erlc main.erl
erl -s main start