#!/bin/bash

go run main.go normal go_skeletons 5 60 10 50 10 skeletons_normal_normal
sleep 5
go run main.go faulty go_skeletons 5 120 30 50 10 skeletons_faulty_normal
sleep 5
go run main.go dynamic go_skeletons 5 120 30 50 10 skeletons_dynamic_normal
sleep 5
go run main.go normal go_wanderer 5 60 10 50 10 wanderer_normal_normal
sleep 5
go run main.go faulty go_wanderer 5 120 30 50 10 wanderer_faulty_normal
sleep 5
go run main.go dynamic go_wanderer 5 120 30 50 10 wanderer_dynamic_normal
sleep 5

./add_net_delay.sh

go run main.go normal go_skeletons 5 60 10 50 10 skeletons_normal_netDelay
sleep 5
go run main.go faulty go_skeletons 5 120 30 50 10 skeletons_faulty_netDelay
sleep 5
go run main.go dynamic go_skeletons 5 120 30 50 10 skeletons_dynamic_netDelay
sleep 5
go run main.go normal go_wanderer 5 60 10 50 10 wanderer_normal_netDelay
sleep 5
go run main.go faulty go_wanderer 5 120 30 50 10 wanderer_faulty_netDelay
sleep 5
go run main.go dynamic go_wanderer 5 120 30 50 10 wanderer_dynamic_netDelay
sleep 5

./rm_net_interferences.sh