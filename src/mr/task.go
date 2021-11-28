package mr

import ("time")

type State int

const (
	Idle       State = 0
	InProgress State = 1
	Finished   State = 2
)

type Task struct {
	Id         int
	InputPaths []string
	OutputPath string
	S          State
	IsReduce   bool
	StartTime  time.Time
}
