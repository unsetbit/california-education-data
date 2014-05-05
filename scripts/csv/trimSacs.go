// This script will take the UserGL.csvs from data/csv/sacs and exclude the "Period" (always "A")
// Colocode (always "BA") and Account (always Fund + Resource + Projectyear + Goal + Function + Object)
// This purely to reduce the size of the CSVs

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
	
	doneChannel := make(chan bool, 10)


	for _, year := range years {
		path, err := filepath.Abs("../../data/csv/sacs/" + year + "/UserGL.csv")
		if err != nil { panic(err) }
		csvReadFile, err := os.Open(path)
		defer csvReadFile.Close()
		if err != nil { panic(err) }
		
		rows := make(chan []string, 1e6)
		go countRecordsInCsv(rows, csvReadFile)

		path, err = filepath.Abs("../../data/csv-cleaned/sacs/UserGL-" + year + ".csv")
		csvWriteFile, err := os.Create(path)
		if err != nil { panic(err) }
		defer csvWriteFile.Close()
		go writeRecordsToCsv(doneChannel, rows, csvWriteFile)
	}


	for i := 0; i < len(years); i++ {
        <-doneChannel
    }

    end := time.Now().UnixNano()
    duration := end - start
    fmt.Println("Read done", duration / 1.0e6)
}

func writeRecordsToCsv(doneChannel chan<- bool, rows <-chan []string, csvFile *os.File){
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	for row := range rows {
		csvWriter.Write(row)
	}

	doneChannel <- true
}

func countRecordsInCsv(rows chan<- []string, csvFile *os.File) {
	csvReader := csv.NewReader(csvFile)
	
	for {
		row, err := csvReader.Read()

		if err == io.EOF { 
			break 
		} else if err != nil { 
			panic(err) 
		}

		rows <- transform(row)
	}

	close(rows)
}

func transform(row []string) []string{
	// Exclude the Period, Colcode, and Account fields (indexes 4-6)
	return []string{row[0], row[1], row[2], row[3], row[7], row[8], row[9], row[10], row[11], row[12], row[13]}
}