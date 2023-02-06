package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/btnguyen2k/gocosmos"
	"github.com/joho/godotenv"
	"github.com/xuri/excelize/v2"
)

// ExcelEmployee types the data extracted from data.xlsx
type ExcelEmployee struct {
        id                                                              int
        activeYN                                                        bool
        name, nextAward, lastAward                                      string
        lastAwardDate, hireDate, termDate, reHireDate, lastAccidentDate time.Time
}

type CosmosEmployee struct {
        id int
		SafetyAwards struct {
				lastAccident string
				notes string
				adminTrack[] string
		}
}

func main() {
		err := godotenv.Load()
		if err != nil {
				log.Fatal("Error loading .env file")
		}
  		driver := "gocosmos"
		dsn := os.Getenv("COSMOS_CONNECTION_STR")
		db, err := sql.Open(driver, dsn)
		if err != nil {
				log.Fatal("Error creating Cosmos connection")
		}
		defer db.Close()
        f, err := excelize.OpenFile("data.xlsx")
		if err != nil {
				log.Fatal("Error opening data.xlsx")
		}
		defer func() {
					if err := f.Close(); err != nil {
						log.Fatal("Error closing data.xlsx")
					}
		}()
		cols, err := f.Cols("DataSheet")
		if err != nil {
				log.Fatal(err)
		}
        list, err := create(cols)
        if err != nil {
				print(list)
				log.Fatalf("\nError creating a list of terminated employees:\t%v", err)
        }
}

// create generates a list of terminated employees
func create(cols *excelize.Cols) ([]ExcelEmployee, error) {
        var err error
        list := make([]ExcelEmployee, 1)
        for cols.Next() {
                col, err := cols.Rows()
                if err != nil {
                        return list, err
                }
                if len(list) != len(col) {
                        list = make([]ExcelEmployee, len(col))
                }
                for i := 1; i < len(col); i++ {
                        val := &list[i-1]
						// some cells are empty or contain a single ".", skip these
                        if len(col[i]) == 0 || col[i] == "." {
                                continue
                        }
                        switch colName := strings.Trim(col[0], " "); colName {
                        case "Employee Name":
                                val.name = col[i]
                        case "Hire Date":
                                val.hireDate = std(col[i])
                        case "Term Date":
                                val.termDate = std(col[i])
                        case "Re Hire Date":
                                val.reHireDate = std(col[i])
                        case "Last Accident":
                                val.lastAccidentDate = std(col[i])
                        case "Term Without Date":
                                val.activeYN = active(val.hireDate, val.termDate, val.reHireDate, col[i])
                        case "Next Award Name":
                                val.nextAward = col[i]
                        case "Award Received":
                                val.lastAwardDate = std(strings.TrimSuffix(col[i], "T00:00:00"))
                        case "Employee Number":
                                val.id = sti(col[i])
                        }
                }
        }
        return filter(list), err
}

// filter creates a new list and only appends terminated employees
func filter(e []ExcelEmployee) []ExcelEmployee {
        newList := make([]ExcelEmployee, 1)
        for _, val := range e {
                if !val.activeYN {
                        newList = append(newList, val)
                }
        }
        return newList
}

// active determines if an employee is active in our system
func active(hire, term, reHire time.Time, termNoDate string) bool {
        z := time.Time{}
        switch {
        case termNoDate == "TRUE":
                return true
        case reHire != z:
                {
                        if term.After(reHire) {
                                return true
                        }
                }
        case term.After(hire):
                return true
        }
        return false
}

// std converts string s to to time.Time
// s will always be a short date format
func std(s string) time.Time {
        format := "2006-01-02"
		// check s to verify that it fits the necessary date format to correctly parse the data
        if len(s) != len(format) {
				log.Fatal(s, "does not match the parse format")
        }
        d, err := time.Parse(format, strings.Trim(s, " "))
        if err != nil {
				log.Fatal(err)
        }
        return d
}

// sti converts string s to type int
func sti(s string) int {
        i, err := strconv.Atoi(s)
        if err != nil {
				log.Fatal(err)
        }
        return i
}

// print is a helper function for printing an array of ExcelEmployee structs
func print(e []ExcelEmployee) {
        for _, val := range e {
                fmt.Printf("ID: %v\n", val.id)
                fmt.Printf("Name: %v\n", val.name)
                fmt.Printf("ActiveYN: %v\n", val.activeYN)
                fmt.Printf("Hire Date: %v\n", val.hireDate)
                fmt.Printf("Term Date: %v\n", val.termDate)
                fmt.Printf("ReHire Date: %v\n", val.reHireDate)
                fmt.Printf("Award Received: %v\n", val.lastAwardDate)
                fmt.Printf("Next Award: %v\n", val.nextAward)
                fmt.Println("--------------------------------------")
        }
}