package dynamodb_client

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"gitlab.com/ptami_lib/util"
	"reflect"
	"strings"
	"time"
)

type QueryOptionOrder struct {
	Field     string `json:"field"`
	Direction string `json:"direction"`
}

type QueryOptionPage struct {
	AllInOne         bool        `json:"allInOne" validate:"required"`
	PageSize         uint        `json:"pageSize" validate:"required"`
	LastEvaluatedKey interface{} `json:"lastEvaluatedKey" validate:"required" default:"{}"`
}

type QueryOption struct {
	Filter           map[string]interface{} `json:"filter"`
	Order            []QueryOptionOrder     `json:"order"`
	ScanIndexForward *bool                  `json:"scanIndexForward"`
	Page             *QueryOptionPage       `json:"page" validate:"required"`
}

type Key struct {
	PK          *string `json:",omitempty" dynamodbav:",omitempty"`
	SK          *string `json:",omitempty" dynamodbav:",omitempty"`
	IndexName   *string `json:",omitempty" dynamodbav:",omitempty"`
	SortKeyType *string `json:",omitempty" dynamodbav:",omitempty"`
}

const (
	KeySortKeyTypeEqualTo              = "="
	KeySortKeyTypeLessThanOrEqualTo    = "<="
	KeySortKeyTypeLessThan             = "<"
	KeySortKeyTypeGreaterThanOrEqualTo = ">="
	KeySortKeyTypeGreaterThan          = ">"
	KeySortKeyTypeBetween              = "between"
	KeySortKeyTypeBeginsWith           = "begins_with"
)

type DynamoDbClient struct {
	dynamoDb  *dynamodb.Client
	tableName string
}

func New(dynamoDb *dynamodb.Client, tableName string) *DynamoDbClient {
	return &DynamoDbClient{
		dynamoDb:  dynamoDb,
		tableName: tableName,
	}
}

func (r *DynamoDbClient) DeleteAllItem() (err error) {
	paginator := dynamodb.NewScanPaginator(r.dynamoDb, &dynamodb.ScanInput{
		TableName: aws.String(r.tableName),
	})

	for paginator.HasMorePages() {
		var out *dynamodb.ScanOutput
		out, err = paginator.NextPage(context.TODO())
		if err != nil {
			return
		}

		for _, item := range out.Items {
			_, err = r.dynamoDb.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
				TableName: aws.String(r.tableName),
				Key: map[string]types.AttributeValue{
					"PK": item["PK"],
					"SK": item["SK"],
				},
			})
			if err != nil {
				return
			}
		}
	}

	return
}

func (r *DynamoDbClient) Insert(item interface{}) (err error) {
	var avItem map[string]types.AttributeValue

	avItem, err = attributevalue.MarshalMap(item)
	if err != nil {
		return
	}

	input := &dynamodb.PutItemInput{
		Item:      avItem,
		TableName: aws.String(r.tableName),
	}

	_, err = r.dynamoDb.PutItem(context.TODO(), input)
	if err != nil {
		return
	}

	return
}

func (r *DynamoDbClient) GetItemList(key Key, arrayOfField string, queryOption QueryOption) (items []map[string]types.AttributeValue, lastEvaluatedKey interface{}, err error) {
	var output *dynamodb.QueryOutput
	var expressionAttributeValues map[string]types.AttributeValue
	var keyConditionExpression string
	var scanIndexForward = queryOption.ScanIndexForward

	if nil == scanIndexForward {
		scanIndexForward = aws.Bool(false)
	}

	expressionAttributeValues = make(map[string]types.AttributeValue)
	expressionAttributeValues[":gsipk"] = &types.AttributeValueMemberS{Value: *key.PK}
	keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk", *key.IndexName)

	if nil != key.SK {
		var sortKeyConditionExpression string

		if nil == key.SortKeyType {
			if strings.HasSuffix(*key.SK, "#") {
				key.SortKeyType = aws.String(KeySortKeyTypeBeginsWith)
			} else {
				key.SortKeyType = aws.String(KeySortKeyTypeEqualTo)
			}
		}

		switch sortKeyType := *key.SortKeyType; sortKeyType {
		case KeySortKeyTypeEqualTo,
			KeySortKeyTypeLessThanOrEqualTo,
			KeySortKeyTypeLessThan,
			KeySortKeyTypeGreaterThanOrEqualTo,
			KeySortKeyTypeGreaterThan,
			KeySortKeyTypeBeginsWith:
			expressionAttributeValues[":gsisk"] = &types.AttributeValueMemberS{Value: *key.SK}
		case KeySortKeyTypeBetween:
			var prefix string
			var begin, end string

			if strings.Contains(*key.SK, "#") {
				var listPrefix = strings.Split(*key.SK, "#")
				prefix = strings.Join(listPrefix[:len(listPrefix)-1], "#")
				var listSection = strings.Split(listPrefix[len(listPrefix)-1], "/")
				begin = fmt.Sprintf("%s#%s", prefix, listSection[0])
				end = fmt.Sprintf("%s#%s", prefix, listSection[1])
			}

			expressionAttributeValues[":gsiskBegin"] = &types.AttributeValueMemberS{Value: begin}
			expressionAttributeValues[":gsiskEnd"] = &types.AttributeValueMemberS{Value: end}
		default:
			err = errors.New(fmt.Sprintf("not supported sort key type (%s)", sortKeyType))
			return
		}

		switch sortKeyType := *key.SortKeyType; sortKeyType {
		case KeySortKeyTypeEqualTo,
			KeySortKeyTypeLessThanOrEqualTo,
			KeySortKeyTypeLessThan,
			KeySortKeyTypeGreaterThanOrEqualTo,
			KeySortKeyTypeGreaterThan:
			sortKeyConditionExpression = fmt.Sprintf("#%sSK %s :gsisk", *key.IndexName, sortKeyType)
		case KeySortKeyTypeBetween:
			sortKeyConditionExpression = fmt.Sprintf("#%sSK %s :gsiskBegin and :gsiskEnd", *key.IndexName, sortKeyType)
		case KeySortKeyTypeBeginsWith:
			sortKeyConditionExpression = fmt.Sprintf("%s(#%sSK, :gsisk)", sortKeyType, *key.IndexName)
		default:
			err = errors.New(fmt.Sprintf("not supported sort key type (%s)", sortKeyType))
			return
		}

		keyConditionExpression = fmt.Sprintf("%s And %s", keyConditionExpression, sortKeyConditionExpression)
	}

	expressionAttributeNames := make(map[string]string)
	expressionAttributeNames[fmt.Sprintf("#%sPK", *key.IndexName)] = fmt.Sprintf("%sPK", *key.IndexName)
	expressionAttributeNames[fmt.Sprintf("#%sSK", *key.IndexName)] = fmt.Sprintf("%sSK", *key.IndexName)

	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		KeyConditionExpression:    aws.String(keyConditionExpression),
		TableName:                 aws.String(r.tableName),
		ScanIndexForward:          scanIndexForward,
	}

	if nil != queryOption.Filter {
		filterExpression, _ := processQueryOptionFilter(queryOption.Filter, expressionAttributeValues, expressionAttributeNames)
		input.FilterExpression = aws.String(filterExpression)
	}

	if nil != queryOption.Page {
		input.Limit = aws.Int32(int32(queryOption.Page.PageSize))

		if reflect.ValueOf(queryOption.Page.LastEvaluatedKey).Len() > 0 {
			var _lastEvaluatedKey map[string]types.AttributeValue

			_lastEvaluatedKey, err = attributevalue.MarshalMap(queryOption.Page.LastEvaluatedKey)
			if err != nil {
				return
			}

			input.ExclusiveStartKey = _lastEvaluatedKey
		}
	}

	if key.IndexName != nil {
		input.IndexName = key.IndexName
	}

	if arrayOfField != "" {
		temp := strings.Split(arrayOfField, ",")
		tempArrayOfField := make([]string, len(temp))

		for i, v := range temp {
			expressionAttributeNames[fmt.Sprintf("#%s", v)] = strings.ReplaceAll(v, "#", "")
			tempArrayOfField[i] = fmt.Sprintf("#%s", v)
		}

		input.ProjectionExpression = aws.String(strings.Join(tempArrayOfField, ","))
	}

query:
	output, err = r.dynamoDb.Query(context.TODO(), input)
	if err != nil {
		return
	}

	//if len(output.Items) < 1 {
	//	err = errors.New(fmt.Sprintf("item not found (%s)", util.StructToString(key)))
	//	return
	//}

	if nil != output.LastEvaluatedKey {
		LastEvaluatedKey := new(map[string]interface{})
		if err = attributevalue.UnmarshalMap(output.LastEvaluatedKey, &LastEvaluatedKey); err != nil {
			return
		}
		lastEvaluatedKey = LastEvaluatedKey
	}

	items = append(items, output.Items...)

	if nil != queryOption.Page && queryOption.Page.AllInOne && nil != output.LastEvaluatedKey {
		goto query
	}

	return
}

//func (r *DynamoDbClient) GetCountList(key Key, arrayOfField string, queryOption QueryOption) (total uint64, err error) {
//	var output *dynamodb.QueryOutput
//	var expressionAttributeValues map[string]types.AttributeValue
//	var keyConditionExpression string
//
//	expressionAttributeValues = make(map[string]types.AttributeValue)
//	expressionAttributeValues[":gsipk"] = &types.AttributeValueMemberS{Value: *key.PK}
//
//	keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk", *key.IndexName)
//
//	if nil != key.SK {
//		expressionAttributeValues[":gsisk"] = &types.AttributeValueMemberS{Value: *key.SK}
//		if strings.HasSuffix(*key.SK, "#") {
//			keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and begins_with(#%sSK, :gsisk)", *key.IndexName, *key.IndexName)
//		} else {
//			keyConditionExpression = fmt.Sprintf("#%sPK = :gsipk and #%sSK = :gsisk", *key.IndexName, *key.IndexName)
//		}
//	}
//
//	expressionAttributeNames := make(map[string]string)
//	expressionAttributeNames[fmt.Sprintf("#%sPK", *key.IndexName)] = fmt.Sprintf("%sPK", *key.IndexName)
//	expressionAttributeNames[fmt.Sprintf("#%sSK", *key.IndexName)] = fmt.Sprintf("%sSK", *key.IndexName)
//
//	input := &dynamodb.QueryInput{
//		ExpressionAttributeNames:  expressionAttributeNames,
//		ExpressionAttributeValues: expressionAttributeValues,
//		KeyConditionExpression:    aws.String(keyConditionExpression),
//		TableName:                 aws.String(r.tableName),
//	}
//
//	if queryOption.Filter != nil {
//		filterExpression, _ := processQueryOptionFilter(queryOption.Filter, expressionAttributeValues, expressionAttributeNames)
//		input.FilterExpression = aws.String(filterExpression)
//	}
//
//	if key.IndexName != nil {
//		input.IndexName = key.IndexName
//	}
//
//	if arrayOfField != "" {
//		temp := strings.Split(arrayOfField, ",")
//		tempArrayOfField := make([]string, len(temp))
//		for i, v := range temp {
//			expressionAttributeNames[fmt.Sprintf("#%s", v)] = strings.ReplaceAll(v, "#", "")
//			tempArrayOfField[i] = fmt.Sprintf("#%s", v)
//		}
//		input.ProjectionExpression = aws.String(strings.Join(tempArrayOfField, ","))
//	}
//
//	output, err = r.dynamoDb.Query(context.TODO(), input)
//	if err != nil {
//		return
//	}
//	if len(output.Items) < 1 {
//		total = 0
//		return
//	} else {
//		total = uint64(len(output.Items))
//	}
//	return
//}

func (r *DynamoDbClient) GetItem(key Key) (item map[string]types.AttributeValue, err error) {
	if nil == key.IndexName {
		var av map[string]types.AttributeValue
		var output *dynamodb.GetItemOutput

		av, err = attributevalue.MarshalMap(key)
		if err != nil {
			return
		}

		input := &dynamodb.GetItemInput{
			Key:       av,
			TableName: aws.String(r.tableName),
		}

		output, err = r.dynamoDb.GetItem(context.TODO(), input)
		if err != nil {
			return
		}

		if nil == output.Item {
			err = errors.New(fmt.Sprintf("item not found (%s)", util.StructToString(key)))
			return
		}

		item = output.Item
	} else {
		item, err = r.getItemViaGsi(key)
	}

	return
}

func (r *DynamoDbClient) getItemViaGsi(key Key) (item map[string]types.AttributeValue, err error) {
	var expressionAttributeValues map[string]types.AttributeValue
	var keyConditionExpression string

	expressionAttributeValues = map[string]types.AttributeValue{
		":gsipk": &types.AttributeValueMemberS{Value: *key.PK},
	}
	keyConditionExpression = fmt.Sprintf("%sPK = :gsipk", *key.IndexName)

	if nil != key.SK {
		expressionAttributeValues[":gsisk"] = &types.AttributeValueMemberS{Value: *key.SK}
		if strings.HasSuffix(*key.SK, "#") {
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and begins_with(%sSK, :gsisk)", *key.IndexName, *key.IndexName)
		} else {
			keyConditionExpression = fmt.Sprintf("%sPK = :gsipk and %sSK = :gsisk", *key.IndexName, *key.IndexName)
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(r.tableName),
		IndexName:                 key.IndexName,
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		Limit:                     aws.Int32(1),
	}

	output, err := r.dynamoDb.Query(context.TODO(), input)
	if err != nil {
		return
	}

	if 0 == len(output.Items) {
		err = errors.New(fmt.Sprintf("item not found (%s)", util.StructToString(key)))
		return
	}

	item = output.Items[0]

	return
}

func (r *DynamoDbClient) DeleteItem(key Key) (err error) {
	var av map[string]types.AttributeValue

	av, err = attributevalue.MarshalMap(key)
	if err != nil {
		return
	}

	input := &dynamodb.DeleteItemInput{
		Key:       av,
		TableName: aws.String(r.tableName),
	}

	_, err = r.dynamoDb.DeleteItem(context.TODO(), input)

	return
}

func (r *DynamoDbClient) UpdateItem(key Key, propertyMap map[string]interface{}) (output *dynamodb.UpdateItemOutput, err error) {
	var keyAv map[string]types.AttributeValue
	var expressionAv map[string]types.AttributeValue
	var expressionAttributeNames = map[string]string{}
	var expressionAttributeValues = map[string]interface{}{}
	var expressionNamesAndValues = map[string]string{}
	var updateExpressions []string

	keyAv, err = attributevalue.MarshalMap(key)
	if err != nil {
		return
	}

	// add UpdatedTimestamp
	propertyMap["UpdatedTimestamp"] = time.Now()

	err = buildExpressionAttributeNamesAndValue(nil, propertyMap, &expressionAttributeNames, &expressionAttributeValues, &expressionNamesAndValues)
	if nil != err {
		return
	}

	expressionAv, err = attributevalue.MarshalMap(expressionAttributeValues)
	if err != nil {
		return
	}

	for k, v := range expressionNamesAndValues {
		updateExpressions = append(updateExpressions, fmt.Sprintf("%s=%s", k, v))
	}

	input := &dynamodb.UpdateItemInput{
		Key:                       keyAv,
		TableName:                 aws.String(r.tableName),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAv,
		UpdateExpression:          aws.String(fmt.Sprintf("set %s", strings.Join(updateExpressions, ", "))),
		ReturnValues:              types.ReturnValueUpdatedNew,
	}

	output, err = r.dynamoDb.UpdateItem(context.TODO(), input)

	return
}

func buildExpressionAttributeNamesAndValue(parentName *[]string, mapData map[string]interface{}, expressionAttributeNames *map[string]string, expressionAttributeValues *map[string]interface{}, expressionNamesAndValues *map[string]string) (err error) {
	for k, v := range mapData {
		var isFunction = 0 == strings.Index(k, "Fn:")
		var keysFunction []string

		if isFunction {
			keysFunction = strings.Split(k, ":") // Fn:function_name:key

			if 3 == len(keysFunction) {
				switch keysFunction[1] { // function_name
				case "list_append":
					k = keysFunction[2]
					//v = fmt.Sprintf("list_append(%s, %s)",
				case "increase", "decrease":
					k = keysFunction[2]
					(*expressionAttributeValues)[":_Zero"] = 0
				default:
					err = errors.New(fmt.Sprintf("unsupported function name (%s)", k))
					return
				}
			} else {
				err = errors.New(fmt.Sprintf("unsupported function format (%s)", k))
				return
			}
		}

		(*expressionAttributeNames)["#"+k] = k

		if !isFunction && reflect.ValueOf(v).Kind() == reflect.Map {
			if nil == parentName {
				parentName = &[]string{k}
			} else {
				if 1 <= len(*parentName) {
					goto build
				}

				_parentName := append(*parentName, k)
				parentName = &_parentName
			}

			err = buildExpressionAttributeNamesAndValue(parentName, v.(map[string]interface{}), expressionAttributeNames, expressionAttributeValues, expressionNamesAndValues)
			if nil != err {
				return
			}

			continue
		}

	build:
		if nil == parentName {
			(*expressionAttributeValues)[fmt.Sprintf(":%s", k)] = v

			if isFunction {
				(*expressionNamesAndValues)[fmt.Sprintf("#%s", k)], err = buildFunctionForExpressionAttributeNamesAndValue(keysFunction[1], k)
				if nil != err {
					return
				}
			} else {
				(*expressionNamesAndValues)[fmt.Sprintf("#%s", k)] = fmt.Sprintf(":%s", k)
			}
		} else {
			(*expressionAttributeValues)[fmt.Sprintf(":%s", strings.Join(append(*parentName, k), "_"))] = v

			if isFunction {
				(*expressionNamesAndValues)[fmt.Sprintf("#%s", strings.Join(append(*parentName, k), ".#"))], err = buildFunctionForExpressionAttributeNamesAndValue(keysFunction[1], k)
				if nil != err {
					return
				}
			} else {
				(*expressionNamesAndValues)[fmt.Sprintf("#%s", strings.Join(append(*parentName, k), ".#"))] = fmt.Sprintf(":%s", strings.Join(append(*parentName, k), "_"))
			}
		}
	}

	return
}

func buildFunctionForExpressionAttributeNamesAndValue(functionName string, key string) (function string, err error) {
	switch functionName {
	case "list_append":
		function = fmt.Sprintf("list_append(%s, :%s)", key, key)
	case "increase":
		function = fmt.Sprintf("if_not_exists(%s, :_Zero) + :%s", key, key)
	case "decrease":
		function = fmt.Sprintf("if_not_exists(%s, :_Zero) - :%s", key, key)
	default:
		err = errors.New(fmt.Sprintf("unsupported function name (%s)", functionName))
	}

	return
}

func processQueryOptionFilter(filter map[string]interface{}, expressionAttributeValues map[string]types.AttributeValue, expressionAttributeName map[string]string) (filterExpression string, err error) {
	if nil == filter {
		return
	}
	var i = 0
	for _, value := range filter {
		item := value.(map[string]interface{})
		field := strings.ReplaceAll(item["field"].(string), ".", "")
		filter := getWhere(item)
		if i == 0 {
			filterExpression += filter
		} else {
			filterExpression += " AND " + filter
		}
		expressionAttributeName["#"+field] = item["field"].(string)
		if item["type"].(string) == "date" {
			date := strings.Split(item["keyword"].(string), "/")
			expressionAttributeValues[":"+field+"Start"] = &types.AttributeValueMemberS{Value: date[0]}
			expressionAttributeValues[":"+field+"End"] = &types.AttributeValueMemberS{Value: date[1]}
		} else if item["type"].(string) != "exist" {
			switch item["keyword"].(type) {
			case bool:
				expressionAttributeValues[":"+field] = &types.AttributeValueMemberBOOL{Value: item["keyword"].(bool)}
			case string:
				expressionAttributeValues[":"+field] = &types.AttributeValueMemberS{Value: item["keyword"].(string)}
			}
		}
		i++
	}

	return
}

func getWhere(item map[string]interface{}) (filterExpression string) {

	field := strings.ReplaceAll(item["field"].(string), ".", "")
	searchType := item["type"].(string)
	field = strings.ReplaceAll(field, "\"", "'")

	switch searchType {
	case "keyword":
		filterExpression = fmt.Sprintf("contains (#%s, :%s)", field, field)
	case "const":
		filterExpression = fmt.Sprintf("#%s = :%s", field, field)
	case "date":
		filterExpression = fmt.Sprintf("#%s BETWEEN :%s AND :%s", field, field+"Start", field+"End")
	case "exist":
		filterExpression = fmt.Sprintf("attribute_exists(#%v)", field)
	}

	return
}
