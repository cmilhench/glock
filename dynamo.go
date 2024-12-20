package glock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Interface assertion
var _ Locker = (*DynamoLock)(nil)

type DynamoClient interface {
	PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type DynamoLock struct {
	client    DynamoClient
	tableName string
	config    Config
}

func NewDynamoLock(client DynamoClient, tableName string, config Config) Locker {
	return &DynamoLock{
		client:    client,
		tableName: tableName,
		config:    config,
	}
}

// Acquire attempts to acquire the lock
func (l *DynamoLock) Acquire(ctx context.Context) (bool, error) {
	now := time.Now().Unix()
	ttl := time.Now().Add(l.config.LockTimeout).Unix()
	_, err := l.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(l.tableName),
		Item: map[string]types.AttributeValue{
			"LockKey":   &types.AttributeValueMemberS{Value: l.config.LockID},
			"LockValue": &types.AttributeValueMemberS{Value: l.config.Owner},
			"TTL":       &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)},
		},
		ConditionExpression: aws.String("attribute_not_exists(LockKey) OR TTL < :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now)},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Release attempts to release the lock
func (l *DynamoLock) Release(ctx context.Context) (bool, error) {
	_, err := l.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(l.tableName),
		Key: map[string]types.AttributeValue{
			"LockKey": &types.AttributeValueMemberS{Value: l.config.LockID},
		},
		ConditionExpression: aws.String("LockValue = :owner"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": &types.AttributeValueMemberS{Value: l.config.Owner},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Renew attempts to renew the lock
func (l *DynamoLock) Renew(ctx context.Context) (bool, error) {
	ttl := time.Now().Add(l.config.LockTimeout).Unix()
	_, err := l.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(l.tableName),
		Item: map[string]types.AttributeValue{
			"LockKey":   &types.AttributeValueMemberS{Value: l.config.LockID},
			"LockValue": &types.AttributeValueMemberS{Value: l.config.Owner},
			"TTL":       &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)},
		},
		ConditionExpression: aws.String("attribute_not_exists(LockKey) OR LockValue = :owner"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": &types.AttributeValueMemberS{Value: l.config.Owner},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isConditionalCheckFailed(err error) bool {
	var conditionCheckFailed *types.ConditionalCheckFailedException
	return errors.As(err, &conditionCheckFailed)
}
