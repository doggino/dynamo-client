package dynamodb_client

import (
	"fmt"
	"strings"
	"time"
)

type DynamoDbMetaData struct {
	PK               string     `json:"-" dynamodbav:",omitempty"`
	SK               string     `json:"-" dynamodbav:",omitempty"`
	GSI1PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI1SK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI2PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI2SK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI3PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI3SK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI4PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI4SK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI5PK           *string    `json:"-" dynamodbav:",omitempty"`
	GSI5SK           *string    `json:"-" dynamodbav:",omitempty"`
	Id               *string    `json:",omitempty" dynamodbav:",omitempty"`
	CreatedTimestamp *time.Time `json:",omitempty" dynamodbav:",omitempty"`
	UpdatedTimestamp *time.Time `json:",omitempty" dynamodbav:",omitempty"`
}

// DynamoDbValueMetaData
// Name
// DisplayOrder
// ParentId
// ValueData
type DynamoDbValueMetaData struct {
	DynamoDbMetaData
	GSIVPK       *string                `json:"-" dynamodbav:",omitempty"`
	GSIVSK       *string                `json:"-" dynamodbav:",omitempty"`
	ParentId     *string                `json:",omitempty" dynamodbav:",omitempty"`
	Name         *string                `json:",omitempty" dynamodbav:",omitempty"`
	DisplayOrder *uint                  `json:",omitempty" dynamodbav:",omitempty"`
	ValueData    map[string]interface{} `json:",omitempty" dynamodbav:",omitempty"`
}

const (
	ValuePk = "VALUE"
	ValueSk = "DISPLAYORDER"
)

func BuildValuePk(entityName string, branchId string, parentId string) string {
	return strings.ToUpper(strings.Join([]string{ValuePk, entityName, branchId, parentId}, "#"))
}

func BuildValueSk(displayOrder *uint) string {
	var displayOrderString string

	if nil != displayOrder {
		displayOrderString = fmt.Sprintf("%04d", displayOrder)
	}

	return strings.ToUpper(strings.Join([]string{ValueSk, displayOrderString}, "#"))
}
