package parser

type StatefulMap struct {
	Data      map[string]*FileStatus
	Started   chan string
	Completed chan string
}

type FileStatus struct {
	Path     string
	Started  bool
	Done     bool
	Progress chan int64
}
