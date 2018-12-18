package unique_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/eventsource-ecosystem/unique"
)

var (
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func TempTable(t *testing.T, api *dynamodb.DynamoDB, fn func(tableName string)) {
	// Create a temporary table for use during this test
	//
	now := strconv.FormatInt(time.Now().UnixNano(), 36)
	random := strconv.FormatInt(int64(r.Int31()), 36)
	tableName := "tmp-" + now + "-" + random
	input := unique.MakeCreateTableInput(tableName, 50, 50)
	_, err := api.CreateTable(input)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	defer func() {
		_, err := api.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	}()

	fn(tableName)
}
