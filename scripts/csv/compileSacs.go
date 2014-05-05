// Takes cleaned csvs and compiles them to one file (UserGL-2003-2013)

package main

import (
    "encoding/csv"
    "path/filepath"
    "os"
    "fmt"
    "time"
    "io"
)

func main() {

	years := [...]string{
		"2003-2004",
		"2004-2005",
		"2005-2006",
		"2006-2007",
		"2007-2008",
		"2008-2009",
		"2009-2010",
		"2010-2011",
		"2011-2012",
		"2012-2013"}

	start := time.Now().UnixNano()
	
	doneChan := make(chan bool, 10)
	rows := make(chan []string, 1e8)

	for _, year := range years {
		path, err := filepath.Abs("../../data/csv-cleaned/sacs/UserGL-" + year + ".csv")
		if err != nil { panic(err) }
		csvReadFile, err := os.Open(path)
		defer csvReadFile.Close()
		if err != nil { panic(err) }
		
		go countRecordsInCsv(doneChan, rows, csvReadFile)
	}

	path, err := filepath.Abs("../../data/csv-cleaned/sacs/UserGL-2003-2013.csv")
	csvWriteFile, err := os.Create(path)
	if err != nil { panic(err) }
	defer csvWriteFile.Close()

	go writeRecordsToCsv(doneChan, rows, csvWriteFile)

	for i := 0; i < len(years); i++ {
        <-doneChan
    }
    close(rows)

    <-doneChan

    end := time.Now().UnixNano()
    duration := end - start
    fmt.Println("Read done", duration / 1.0e6)
}

func writeRecordsToCsv(doneChan chan<- bool, rows <-chan []string, csvFile *os.File){
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	for row := range rows {
		csvWriter.Write(row)
	}

	doneChan <- true
}

func countRecordsInCsv(doneChan chan<- bool, rows chan<- []string, csvFile *os.File) {
	csvReader := csv.NewReader(csvFile)
	csvReader.Read() // disregard header
	for {
		row, err := csvReader.Read()

		if err == io.EOF { 
			break 
		} else if err != nil { 
			panic(err) 
		}

		rows <- row
	}

	doneChan <- true
}
