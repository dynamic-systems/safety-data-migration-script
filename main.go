package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/joho/godotenv"
	"github.com/xuri/excelize/v2"
)

type ExcelEmployee struct {
	activeYN                                                        bool
	id, name, lastAward, nextAward, track                           string
	lastAwardDate, hireDate, termDate, reHireDate, lastAccidentDate time.Time
}

type Award struct {
	Step         int     `json:"step"`
	ReceiptId    *string `json:"receiptId"`
	ReceivedDate *string `json:"receivedDate"`
}

type AdminTrack struct {
	Zero Award `json:"0"`
	One  Award `json:"1"`
}
type FieldTrack struct {
	Zero  Award `json:"0"`
	One   Award `json:"1"`
	Two   Award `json:"2"`
	Three Award `json:"3"`
	Four  Award `json:"4"`
	Five  Award `json:"5"`
	Six   Award `json:"6"`
}

type SafetyAwards struct {
	LastAccident *string    `json:"lastAccident"`
	Notes        string     `json:"notes"`
	Admin        AdminTrack `json:"adminTrack"`
	Field        FieldTrack `json:"fieldTrack"`
}

type CosmosEmployee struct {
	Id     string       `json:"id"`
	Safety SafetyAwards `json:"safetyAwards"`
	Key    string       `json:"_partitionKey"`
}

var dateErrors = make([]string, 1)

func handle(err error, message string) {
	if err != nil {
		log.Fatalf("%s\n%v", message, err.Error())
	}
}

func main() {
	logs, err := os.OpenFile("out.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	handle(err, "Failed to open out.log")
	defer func() {
		err := logs.Close()
		handle(err, "Failed to close out.log")
	}()
	log.SetOutput(logs)
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	f, err := excelize.OpenFile("data.xlsx")
	handle(err, "Failed to open data.xlsx")
	defer func() {
		err := f.Close()
		handle(err, "Failed to close data.xlsx")
	}()
	log.Println("Connection to data.xlsx has been created")
	cols, err := f.Cols("DataSheet")
	handle(err, "Failed to extract columns from DataSheet")
	list := createEmployeeList(cols)
	log.Printf("List of %v ExcelEmployee structs has been created\n", len(list))
	if len(dateErrors) != 1 {
		for _, d := range dateErrors {
			log.Printf("%s\n", d)
		}
		log.Fatal("Migration failed. Poor data formatting")
	}
	sendToCosmos(list)
}

// 'sendToCosmos' iterates through the list of employees and generates items to be sent to a CosmosDB container. The data is to be used in the Safety Awards Application (https://github.com/dynamic-systmems/safety-awards-list)
func sendToCosmos(list []ExcelEmployee) {
	// load .env
	err := godotenv.Load()
	handle(err, "Failed to load .env")

	// create CosmosDB credentials
	endpoint := os.Getenv("AZURE_COSMOS_ENDPOINT")
	key := os.Getenv("AZURE_COSMOS_KEY")
	cred, err := azcosmos.NewKeyCredential(key)
	handle(err, "Failed to generate Azure credentials")
	log.Println("Azure credentials approved")

	// create CosmosDB client
	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	handle(err, "Failed to create CosmosDB client")
	log.Println("CosmosDB client has been created")

	// create Container instance to perform read-write operations
	container, err := client.NewContainer("vaporwave", "employees")
	handle(err, "Failed to create container")
	log.Println("Container has been created")

	// generate a PartitionKey
	pk := azcosmos.NewPartitionKeyString("")

	// create items and send to the container
	for _, emp := range list {
		item := buildJson(emp)
		marshalled, err := json.Marshal(item)
		handle(err, "Failed to encode item into JSON")
		// create container item
		itemResponse, err := container.CreateItem(context.Background(), pk, marshalled, nil)
		if err != nil {
			var responseErr *azcore.ResponseError
			errors.As(err, &responseErr)
			log.Fatal(responseErr)
		}
		log.Printf("%+v\n", item)
		log.Fatalf("Item created. ActivityId %s consuming %v RU...\n", itemResponse.ActivityID, itemResponse.RequestCharge)
	}
}

// 'buildJson' converts an ExcelEmployee struct into a CosmosEmployee to correctly format each item to be sent to the CosmosDB container
func buildJson(emp ExcelEmployee) CosmosEmployee {
	var lastAccident *string
	zeroDate := time.Time{}
	if emp.lastAccidentDate != zeroDate {
		lastAccident = formatDate(emp.lastAccidentDate)
	}
	var admin AdminTrack
	admin.Zero.Step = 1
	admin.One.Step = 2
	var field FieldTrack
	field.Zero.Step = 1
	field.One.Step = 2
	field.Two.Step = 3
	field.Three.Step = 4
	field.Four.Step = 5
	field.Five.Step = 6
	field.Six.Step = 7
	if emp.track == "admin" {
		switch strings.ToLower(emp.lastAward) {
		case "lunchbox":
			admin.Zero.ReceivedDate = formatDate(emp.lastAwardDate)
		case "set":
			admin.One.ReceivedDate = formatDate(emp.lastAwardDate)
		}
	} else {
		switch strings.ToLower(emp.lastAward) {
		case "cap":
			field.Zero.ReceivedDate = formatDate(emp.lastAwardDate)
		case "lunchbox":
			field.One.ReceivedDate = formatDate(emp.lastAwardDate)
		case "backpack":
			field.Two.ReceivedDate = formatDate(emp.lastAwardDate)
		case "multitool":
			field.Three.ReceivedDate = formatDate(emp.lastAwardDate)
		case "knife":
			field.Four.ReceivedDate = formatDate(emp.lastAwardDate)
		case "set":
			field.Five.ReceivedDate = formatDate(emp.lastAwardDate)
		case "$750":
			field.Six.ReceivedDate = formatDate(emp.lastAwardDate)
		}
	}
	cosmos := CosmosEmployee{
		Id: emp.id,
		Safety: SafetyAwards{
			LastAccident: lastAccident,
			Notes:        "",
			Admin:        admin,
			Field:        field,
		},
		Key: "",
	}
	return cosmos
}

// 'create' generates a list of terminated ExcelEmployee structs
func createEmployeeList(cols *excelize.Cols) []ExcelEmployee {
	list := make([]ExcelEmployee, 1)
	for cols.Next() {
		col, err := cols.Rows()
		handle(err, "Failed to extract rows from the cols instance")
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
				{
					name := col[i]
					if strings.Contains(col[i], "(") {
						val.track = "admin"
						re := regexp.MustCompile(`\(\w+\)`)
						name = re.ReplaceAllString(name, "")
					} else {
						val.track = "field"
					}
					val.name = name
				}
			case "Hire Date":
				val.hireDate = formatStr(col[i])
			case "Term Date":
				val.termDate = formatStr(col[i])
			case "Re Hire Date":
				val.reHireDate = formatStr(col[i])
			case "Last Accident":
				val.lastAccidentDate = formatStr(col[i])
			case "Term Without Date":
				val.activeYN = isActive(val.hireDate, val.termDate, val.reHireDate, col[i])
			case "Next Award Name":
				{
					lastAward := ""
					if val.track == "admin" {
						switch strings.ToLower(col[i]) {
						case "lunchbox":
							lastAward = "none"
						case "set":
							lastAward = "lunchbox"
						case "done":
							lastAward = "set"
						}
					} else {
						switch strings.ToLower(col[i]) {
						case "cap":
							lastAward = "none"
						case "lunchbox":
							lastAward = "cap"
						case "backpack":
							lastAward = "lunchbox"
						case "multitool":
							lastAward = "backpack"
						case "knife":
							lastAward = "multitool"
						case "set":
							lastAward = "knife"
						case "$750":
							lastAward = "set"
						case "done":
							lastAward = "$750"
						}
					}
					val.lastAward = lastAward
					val.nextAward = col[i]
				}
			case "Award Received":
				{
					date := strings.TrimSuffix(col[i], "T00:00:00")
					val.lastAwardDate = formatStr(date)
				}
			case "Employee Number":
				val.id = col[i]
			}
		}
	}
	return filterActiveEmployees(list)
}

// 'filterActiveEmployees' creates a new list and only appends terminated employees
func filterActiveEmployees(e []ExcelEmployee) []ExcelEmployee {
	newList := make([]ExcelEmployee, 1)
	for _, val := range e {
		if !val.activeYN {
			newList = append(newList, val)
		}
	}
	return newList[1:]
}

// 'isActive' determines if an employee is active in our system
func isActive(hire, term, reHire time.Time, termNoDate string) bool {
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

// 'formatDate' returns the date 'd' as a string in a MM-DD-YYYY format
func formatDate(d time.Time) *string {
	s := d.Format("01-02-2006")
	s = strings.ReplaceAll(s, "-", "/")
	return &s
}

// 'formatStr' returns string 's' as time.Time
// 's' will always be a short date format
func formatStr(s string) time.Time {
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
