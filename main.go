package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// SafetyEmployee defines the data values that will be sent to Azure Blob Storage
type SafetyEmployee struct {
        id                                                                                                int
        name, dept, branch, awardTrack, nextAward, lastAward, employeeType, lastJobWorkedName, occupation string
        nextAwardDate, lastAwardDate, hireDate, termDate, reHireDate, lastAccidentDate, prEndDate         time.Time
        activeYN                                                                                          bool
}

func main() {
        f, err := excelize.OpenFile("data.xlsx")
        if err != nil {
                panic(err)
        }
        defer func() {
                if err := f.Close(); err != nil {
                        panic(err)
                }
        }()
        cols, err := f.Cols("DataSheet")
        if err != nil {
                panic(err)
        }
		list := create(cols)
        print(list)
}

// create generates a list of active employees
func create(cols *excelize.Cols) []SafetyEmployee {
		list := make([]SafetyEmployee, 1)
        for cols.Next() {
                col, err := cols.Rows()
                if len(list) != len(col) {
                        list = make([]SafetyEmployee, len(col))
                }
                if err != nil {
                        panic(err) 
                }
                for i := 1; i < len(col); i++ {
                        obj := &list[i-1]
                        if len(col[i]) == 0 || col[i] == "." {
                                continue
                        }
                        switch colName := strings.Trim(col[0], " "); colName {
                        case "Employee Name":
                                obj.name = col[i]
                        case "Employee Location":
                                obj.dept = col[i]
                        case "Hire Date":
                                obj.hireDate = std(col[i])
                        case "Term Date":
                                obj.termDate = std(col[i])
                        case "Re Hire Date":
                                obj.reHireDate = std(col[i])
                        case "Last Accident":
                                obj.lastAccidentDate = std(col[i])
                        case "Term Without Date":
                                obj.activeYN = active(obj.hireDate, obj.termDate, obj.reHireDate, col[i])
                        case "Next Award Name":
                                obj.nextAward = col[i]
                        case "Next Award Date":
                                obj.nextAwardDate = std(col[i])
                        case "Employee Number":
                                obj.id = sti(col[i])
                        }
                }
        }
		return filter(list)
}

// filter creates a new list and only appends active employees
func filter(e []SafetyEmployee) []SafetyEmployee {
        newList := make([]SafetyEmployee, 1)
        for _, obj := range e {
                if obj.activeYN {
                        newList = append(newList, obj)
                }
        }
        return newList
}

// active determines if an employee is active in our system
func active(hire time.Time, term time.Time, reHire time.Time, termNoDate string) bool {
        z := time.Time{}
        if termNoDate == "TRUE" {
                return false
        }
        if reHire != z {
                if term.After(reHire) {
                        return false
                }
        }
        if term.After(hire) {
                return false
        }
        return true
}

// std converts string s to type time.Time
func std(s string) time.Time {
        d, err := time.Parse("2006-01-02", strings.Trim(s, " "))
        if err != nil {
                panic(err)
        }
        return d
}

// sti converts string s to type int
func sti(s string) int {
        i, err := strconv.Atoi(s)
        if err != nil {
                panic(err)
        }
        return i
}

// print is a helper function for printing an array of SafetyEmployee structs
func print(e []SafetyEmployee) {
        for _, obj := range e {
                fmt.Println("--------------------------------------")
                fmt.Printf("ID: %v\n", obj.id)
                fmt.Printf("Name: %v\n", obj.name)
                fmt.Printf("Dept: %v\n", obj.dept)
                fmt.Printf("Hire Date: %v\n", obj.hireDate)
                fmt.Printf("Term Date: %v\n", obj.termDate)
                fmt.Printf("ReHire Date: %v\n", obj.reHireDate)
                fmt.Printf("ActiveYN: %v\n", obj.activeYN)
                fmt.Println("--------------------------------------")
        }
}