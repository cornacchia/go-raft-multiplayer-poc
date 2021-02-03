module go_wanderer

go 1.15

replace go_raft => ../go_raft

require (
	github.com/sirupsen/logrus v1.7.0
	go_raft v0.0.0-00010101000000-000000000000
	golang.org/x/exp v0.0.0-20210201131500-d352d2db2ceb
	golang.org/x/image v0.0.0-20201208152932-35266b937fa6
	golang.org/x/mobile v0.0.0-20201217150744-e6ae53a27f4f
)
