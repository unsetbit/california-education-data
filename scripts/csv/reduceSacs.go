// This script reduces multi-year csv files into a single denormalized csv file
// ends up being about half the total size, though with gzipping the difference is
// not very significant

package main

import (
    "encoding/csv"
    "path/filepath"
    "os"
    "fmt"
    "time"
    "io"
    "strings"
)

type SacsRow struct {
	Ccode string
	Dcode string
	SchoolCode string
	Fund string
	Resource string
	Projectyear string
	Goal string
	Function string
	Object string
	Value_2003 string
	Value_2004 string
	Value_2005 string
	Value_2006 string
	Value_2007 string
	Value_2008 string
	Value_2009 string
	Value_2010 string
	Value_2011 string
	Value_2012 string
}

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
	
	sacs := make(map[string]*SacsRow)

	start := time.Now().UnixNano()
	
	for _, year := range years {
		path, err := filepath.Abs("../../data/csv/sacs/" + year + "/UserGL.csv")
		if err != nil { panic(err) }
		csvReadFile, err := os.Open(path)
		defer csvReadFile.Close()
		if err != nil { panic(err) }
		
		countRecordsInCsv(csvReadFile, sacs)
	}

	path, err := filepath.Abs("../../data/csv-cleaned/sacs/UserGL-2003-2013-compiled.csv")
	csvWriteFile, err := os.Create(path)
	if err != nil { panic(err) }
	defer csvWriteFile.Close()
	writeCsv(csvWriteFile, sacs)

    end := time.Now().UnixNano()
    duration := end - start
    fmt.Println("Read done", duration / 1.0e6)
}

func writeCsv(csvFile *os.File, sacs map[string]*SacsRow){
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	for _, sacsRow := range sacs {
		csvWriter.Write([]string{
			sacsRow.Ccode,
			sacsRow.Dcode,
			sacsRow.SchoolCode,
			sacsRow.Fund,
			sacsRow.Resource,
			sacsRow.Projectyear,
			sacsRow.Goal,
			sacsRow.Function,
			sacsRow.Object,
			sacsRow.Value_2003,
			sacsRow.Value_2004,
			sacsRow.Value_2005,
			sacsRow.Value_2006,
			sacsRow.Value_2007,
			sacsRow.Value_2008,
			sacsRow.Value_2009,
			sacsRow.Value_2010,
			sacsRow.Value_2011,
			sacsRow.Value_2012,
		})
	}
}

func countRecordsInCsv(csvFile *os.File, sacs map[string]*SacsRow) {
	csvReader := csv.NewReader(csvFile)
	
	for {
		row, err := csvReader.Read()

		if err == io.EOF { 
			break 
		} else if err != nil { 
			panic(err) 
		}

		transform(row, sacs)
	}

}

func transform(row []string, sacs map[string]*SacsRow) []string{
	key := makeKey(row)
	
	if sacsRow, ok := sacs[key]; ok {
		setYearValue(sacsRow, row)
	} else {
		sacsRow := new(SacsRow)
		sacsRow.Ccode = row[0]
		sacsRow.Dcode = row[1]
		sacsRow.SchoolCode = row[2]
		sacsRow.Fund = row[7]
		sacsRow.Resource = row[8]
		sacsRow.Projectyear = row[9]
		sacsRow.Goal = row[10]
		sacsRow.Function = row[11]
		sacsRow.Object = row[12]
		setYearValue(sacsRow, row)
		sacs[key] = sacsRow
	}
	
	
	// Exclude the Period, Colcode, and Account fields (indexes 4-6)
	return []string{row[0], row[1], row[2], row[3], row[7], row[8], row[9], row[10], row[11], row[12], row[13]}
}

func makeKey(row []string) string{
	return strings.Join(row[0:3], "") + strings.Join(row[7:13], "")
}

func setYearValue(sacsRow *SacsRow, row []string){
	value := string(row[13])
	fiscalYear := string(row[3])

	switch {
	case fiscalYear == "2003":
		sacsRow.Value_2003 = value
	case fiscalYear == "2004":
		sacsRow.Value_2004 = value
	case fiscalYear == "2005":
		sacsRow.Value_2005 = value
	case fiscalYear == "2006":
		sacsRow.Value_2006 = value
	case fiscalYear == "2007":
		sacsRow.Value_2007 = value
	case fiscalYear == "2008":
		sacsRow.Value_2008 = value
	case fiscalYear == "2009":
		sacsRow.Value_2009 = value
	case fiscalYear == "2010":
		sacsRow.Value_2010 = value
	case fiscalYear == "2011":
		sacsRow.Value_2011 = value
	case fiscalYear == "2012":
		sacsRow.Value_2012 = value
	}
}