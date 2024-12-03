package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

type LogEntry struct {
	TimeStamp string
	Level     string
	Message   string
}

func readFromfile(fileName string, lines chan<- string) {
	defer close(lines) // Ensure the channel is closed when done
	file, err := os.Open(fileName)
	if err != nil {
		close(lines)
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		//time.Sleep(2 * time.Second)
		lines <- line
		fmt.Println(fmt.Sprintf("Sent by readFromfile to channel:%s", line))
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error Reading lines:", err)
	}
}

func extract(lines <-chan string, out chan<- LogEntry) {
	defer close(out)
	for line := range lines {
		fmt.Println("Recived by extract From channel", line)
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

func transform(in <-chan LogEntry, out chan<- LogEntry) {

	defer close(out)
	for log := range in {
		fmt.Println("Recived by transform From channel", log.Message)
		if log.Level == "ERROR" {
			out <- log
		}
	}
}

func writeToLog(in <-chan LogEntry, wg *sync.WaitGroup) {
	defer wg.Done() // Decrement the counter when done

	for logEntry := range in {
		fmt.Println("Recived by writeToLog From channel", logEntry.Message)
		file, err := os.OpenFile("./error.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

func main() {
	// read from the log file
	logs := "./big_logs.txt"
	lines := make(chan string, 20)
	logEntries := make(chan LogEntry, 20)
	errorEntries := make(chan LogEntry, 20)
	var wg sync.WaitGroup
	go readFromfile(logs, lines)
	// convert to struct
	go extract(lines, logEntries)
	// clean
	go transform(logEntries, errorEntries)
	// write
	wg.Add(1)
	go writeToLog(errorEntries, &wg)
	wg.Wait()

}
