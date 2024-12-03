package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

type LogEtl interface {
	ReadFromFile(fileName string, lines chan<- string)
	Extract(lines <-chan string, out chan<- LogEntry)
	Transform(in <-chan LogEntry, out chan<- LogEntry)
	WriteToLog(in <-chan LogEntry, wg *sync.WaitGroup)
}
type EtlStruct struct {
	ReadLogFile  string
	WriteLogFile string
}

func (c EtlStruct) ReadFromFile(lines chan<- string) {
	defer close(lines) // Ensure the channel is closed when done
	file, err := os.Open(c.ReadLogFile)
	if err != nil {
		close(lines)
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		lines <- line
		fmt.Println(fmt.Sprintf("Sent by readFromfile to channel:%s", line))
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error Reading lines:", err)
	}
}

func (c EtlStruct) Extract(lines <-chan string, out chan<- LogEntry) {
	defer close(out)
	for line := range lines {
		fmt.Println("Received by extract From channel", line)
		parts := strings.SplitN(line, "|", 3)
		if len(parts) == 3 {
			out <- LogEntry{
				TimeStamp: parts[0],
				Level:     parts[1],
				Message:   parts[2],
			}
		}

	}

}

func (c EtlStruct) Transform(in <-chan LogEntry, out chan<- LogEntry) {

	defer close(out)
	for log := range in {
		fmt.Println("Received by transform From channel", log.Message)
		if log.Level == "ERROR" {
			out <- log
		}
	}
}

func (c EtlStruct) WriteToLog(in <-chan LogEntry, wg *sync.WaitGroup) {
	defer wg.Done() // Decrement the counter when done

	for logEntry := range in {
		fmt.Println("Received by writeToLog From channel", logEntry.Message)
		file, err := os.OpenFile(c.WriteLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		_, err = writer.WriteString(fmt.Sprintf("%s|%s|%s\n", logEntry.TimeStamp, logEntry.Level, logEntry.Message))
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
		writer.Flush()
	}
}
