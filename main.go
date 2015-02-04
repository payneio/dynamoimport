package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/dynamodb"
	"log"
	"os"
	"strings"
	"time"
)

var config struct {
	BatchSize      int
	WriteThreshold int
	AccessKey      string
	SecretKey      string
	MappingString  string
	Mapping        []string
	Table          string
	Key            string
	Test           bool
}

type BatchResponse struct {
	Attempted []PostItem
	Failed    []PostItem
}

type PostItem map[string]dynamodb.AttributeValue

func configure() {
}

func getopt(name string, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
}

func init() {
	flag.IntVar(&config.BatchSize, "batchsize", 25, "How many lines to batch together into a single AWS PUT")
	flag.IntVar(&config.WriteThreshold, "writes", 1000, "How many writes/second you have provisioned for your DynamoDB table.")
	flag.StringVar(&config.Table, "table", "my-table", "DynamodDB table destination.")
	flag.BoolVar(&config.Test, "test", false, "testing mode")
	flag.StringVar(&config.MappingString, "mapping", "Attribute1,Attribute2,Attribute3", "Comma-separated list of DynamoDB attribute names to map to source columns.")
	flag.StringVar(&config.AccessKey, "aws-access-key", "", "AWS access key (by default, set from AWS_ACCESS_KEY environment variable.")
	flag.StringVar(&config.AccessKey, "aws-secret-key", "", "AWS secret key (by default, set from AWS_SECRET_KEY environment variable.")
}

func main() {

	flag.Parse()
	config.AccessKey = getopt("AWS_ACCESS_KEY", config.AccessKey)
	config.SecretKey = getopt("AWS_SECRET_KEY", config.SecretKey)
	config.Mapping = strings.Split(config.MappingString, ",")
	if len(flag.Args()) != 1 {
		fmt.Println("You must specify an input file.")
		os.Exit(1)
	}
	fs := flag.Args()[0]

	if config.Test {
		log.Printf("Test mode, configuration: %v\n", config)
	}

	// Start up sub-processes
	batchChannel := make(chan []PostItem)
	batchResponseChannel := make(chan BatchResponse)
	go postBatches(batchChannel, batchResponseChannel)
	go batchResponder(batchResponseChannel)

	// Open files

	inputFile, err := os.Open(fs)
	if err != nil {
		log.Fatalf("Could not open source file: %v", err)
	}
	defer inputFile.Close()

	malformedFile, err := os.OpenFile("malformed.data", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer malformedFile.Close()

	// Scan through file, batch up good lines to the batchChannel

	scanner := bufio.NewScanner(inputFile)

	batch := []PostItem{}

	b := 0
	for scanner.Scan() {
		line := scanner.Text()
		if config.Test {
			log.Printf("Reading log: %v\n", line)
		}
		item, err := getItem(line)
		if err != nil {
			malformedFile.WriteString(fmt.Sprintf("%s\n", line))
			continue
		}
		batch = append(batch, item)
		if len(batch) == config.BatchSize {
			b++
			end := b * config.BatchSize
			start := end - config.BatchSize + 1
			log.Printf("Batching lines %v through %v.\n", start, end)
			batchChannel <- batch
			batch = []PostItem{}
		}
	}
	if len(batch) > 0 {
		batchChannel <- batch
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("Waiting to receive all responses.")
	select {}
}

// getItem takes a Ctrl-31 delimited string and maps it to an PostItem
func getItem(input string) (PostItem, error) {
	item := make(PostItem)
	attrs := strings.Split(input, "")
	if len(attrs) != len(config.Mapping) {
		return item, errors.New(fmt.Sprintf("The input string does not have the same number of attributes (%v) as the mapping (%v).\n", len(attrs), len(config.Mapping)))
	}
	for i, attr := range attrs {
		if attr != "" {
			item[config.Mapping[i]] = dynamodb.AttributeValue{S: aws.String(attr)}
		}
	}
	return item, nil
}

// getLine creates a Ctrl-31 delimited string from a PostItem
func getLine(item PostItem) (string, error) {
	s := []string{}
	for _, attr := range config.Mapping {
		if attrValue, ok := item[attr]; ok {
			s = append(s, *attrValue.S)
		} else {
			s = append(s, "")
		}
	}
	return strings.Join(s, ""), nil
}

func printItem(item PostItem) (string, error) {
	s := []string{}
	for _, attr := range config.Mapping {
		val := ""
		if attrValue, ok := item[attr]; ok {
			val = *attrValue.S
		}
		s = append(s, fmt.Sprintf("\"%s\": \"%s\"", attr, val))
	}
	return fmt.Sprintf("{ %s }", strings.Join(s, ",")), nil
}

func postBatches(batchChannel <-chan []PostItem, batchResponseChannel chan<- BatchResponse) {

	interval := time.Second * time.Duration(config.BatchSize) / time.Duration(config.WriteThreshold)
	ticker := time.NewTicker(interval)
	for {
		<-ticker.C
		batch := <-batchChannel
		postBatch(batch, batchResponseChannel)
	}

}

func postBatch(batch []PostItem, batchResponseChannel chan<- BatchResponse) {

	creds := aws.Creds(config.AccessKey, config.SecretKey, "")
	client := dynamodb.New(creds, "us-west-2", nil)

	// Create batch POST request
	writeRequests := []dynamodb.WriteRequest{}
	for _, item := range batch {
		putRequest := &dynamodb.PutRequest{Item: item}
		writeRequest := dynamodb.WriteRequest{PutRequest: putRequest}
		writeRequests = append(writeRequests, writeRequest)
		if config.Test {
			itemString, _ := printItem(item)
			log.Printf("POST: %v\n", itemString)
		}
	}
	var requestItems = make(map[string][]dynamodb.WriteRequest)
	requestItems[config.Table] = writeRequests
	req := &dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	}

	go func() {
		var failed []PostItem

		if config.Test {
			return
		}

		resp, err := client.BatchWriteItem(req)
		if err != nil {
			log.Printf("Batch POST failed: %v\n", err)
			failed = batch
		} else {
			writeRequests := resp.UnprocessedItems[config.Table]
			failed := []PostItem{}
			for _, writeRequest := range writeRequests {
				failed = append(failed, writeRequest.PutRequest.Item)
			}
		}
		batchResponseChannel <- *&BatchResponse{Attempted: batch, Failed: failed}
	}()

}

func batchResponder(batchResponseChannel <-chan BatchResponse) {

	attemptedFile, err := os.OpenFile("attempted.data", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer attemptedFile.Close()

	failedFile, err := os.OpenFile("fail.data", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer failedFile.Close()

	for {
		batchResponse := <-batchResponseChannel
		// log.Println("Batch response received.")

		for _, item := range batchResponse.Attempted {
			line := *item[config.Mapping[0]].S
			attemptedFile.WriteString(fmt.Sprintf("%s\n", line))
		}

		for _, item := range batchResponse.Failed {
			line, err := getLine(item)
			if err != nil {
				log.Println(err)
			}
			failedFile.WriteString(fmt.Sprintf("%s\n", line))
		}
	}

}
