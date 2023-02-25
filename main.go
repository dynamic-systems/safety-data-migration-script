package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/joho/godotenv"
	"github.com/xuri/excelize/v2"
)

var dateErrors = make([]string, 1)

// ExcelEmployee types the data extracted from data.xlsx
type ExcelEmployee struct {
	id                                                              int
	activeYN                                                        bool
	name, nextAward, lastAward                                      string
	lastAwardDate, hireDate, termDate, reHireDate, lastAccidentDate time.Time
}

func handle(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {
	logs, err := os.OpenFile("tmp.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	handle(err)
	defer func() {
		err := logs.Close()
		handle(err)
	}()
	log.SetOutput(logs)
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	f, err := excelize.OpenFile("data.xlsx")
	handle(err)
	defer func() {
		err := f.Close()
		handle(err)
	}()
	log.Println("Connection to data.xlsx has been created...")
	cols, err := f.Cols("DataSheet")
	handle(err)
	list := create(cols)
	handle(err)
	log.Printf("List of %v ExcelEmployee structs has been created...\n", len(list))
	if len(dateErrors) != 1 {
		for _, d := range dateErrors {
			log.Printf("%s\n", d)
		}
		log.Fatal("Migration failed. Poor data formatting.")
	}
	send()
}

// 'send' iterates through the list of employees and generates items to be sent to a CosmosDB container. The data is to be used in the Safety Awards Application (https://github.com/dynamic-systmems/safety-awards-list)
func send() {
	// load .env
	err := godotenv.Load()
	handle(err)

	// create CosmosDB credentials
	endpoint := os.Getenv("AZURE_COSMOS_ENDPOINT")
	key := os.Getenv("AZURE_COSMOS_KEY")
	cred, err := azcosmos.NewKeyCredential(key)
	handle(err)
	log.Println("Azure credentials approved...")

	// create CosmosDB client
	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	handle(err)
	log.Println("CosmosDB client has been created...")

	// create Container instance to perform read-write operations
	container, err := client.NewContainer("vaporwave", "employees")
	handle(err)
	log.Println("Container has been created...")

	// generate a PartitionKey and example item
	pk := azcosmos.NewPartitionKeyString("")
	item := map[string]string{
		"id":            "1",
		"value":         "2",
		"_partitionKey": "",
	}
	marshalled, err := json.Marshal(item)
	handle(err)

	// create container item
	itemResponse, err := container.CreateItem(context.Background(), pk, marshalled, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		log.Fatal(responseErr)
	}
	log.Printf("Item created. ActivityId %s consuming %v RU...\n", itemResponse.ActivityID, itemResponse.RequestCharge)
}

// 'create' generates a list of terminated employees
func create(cols *excelize.Cols) ([]ExcelEmployee) {
	list := make([]ExcelEmployee, 1)
	for cols.Next() {
		col, err := cols.Rows()
		handle(err)
		if len(list) != len(col) {
			list = make([]ExcelEmployee, len(col))
		}
		for i := 1; i < len(col); i++ {
			val := &list[i-1]
			// some cells are empty or contain a single "." or " ", skip these
			if len(col[i]) == 0 || len(col[i]) == 1 {
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
				{
					date := strings.TrimSuffix(col[i], "T00:00:00")
					val.lastAwardDate = std(date)
				}
			case "Employee Number":
				val.id = sti(col[i])
			}
		}
	}
	return filter(list)
}

// 'filter' creates a new list and only appends terminated employees
func filter(e []ExcelEmployee) []ExcelEmployee {
	newList := make([]ExcelEmployee, 1)
	for _, val := range e {
		if !val.activeYN {
			newList = append(newList, val)
		}
	}
	return newList
}

// 'active' determines if an employee is active in our system
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

// 'std' converts string 's' to to time.Time
// 's' will always be a short date format
func std(s string) time.Time {
	format := "2006-01-02"
	if len(s) != len(format) {
		dateErrors = append(dateErrors, s)
		return time.Time{}
	}
	d, err := time.Parse(format, strings.Trim(s, " "))
	if err != nil {
		dateErrors = append(dateErrors, s)
		return time.Time{}
	}
	return d
}

// 'sti' converts string 's' to type int
func sti(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		dateErrors = append(dateErrors, s)
		return 0
	}
	return i
}
