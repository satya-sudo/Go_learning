package main

import (
	"fmt"
	"sync"
)

type LogEntry struct {
	TimeStamp string
	Level     string
	Message   string
}

func main() {
	readFromLogFile := "./big_logs.txt"
	writeToLogFile := "./errors.txt"
	lines := make(chan string, 20)
	logEntries := make(chan LogEntry, 20)
	errorEntries := make(chan LogEntry, 20)
	var wg sync.WaitGroup
	etl := EtlStruct{readFromLogFile, writeToLogFile}
	go etl.ReadFromFile(lines)
	// convert to struct
	go etl.Extract(lines, logEntries)
	// clean
	go etl.Transform(logEntries, errorEntries)
	// write
	wg.Add(1)
	go etl.WriteToLog(errorEntries, &wg)
	wg.Wait()
	fmt.Sprintf("All Errors from the logs separated")
}
