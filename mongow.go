// Package mongow mongo driver wrapper with useful methods for my purposes
package mongow

import (
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func getFieldByName(itemValue reflect.Value, name string) (reflect.Value, error) {
	if itemValue.Kind() != reflect.Ptr {
		return reflect.Value{}, fmt.Errorf("getFieldByName: item should be a ptr to struct")
	}
	if itemValue.Elem().Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("getFieldByName: item should be a ptr to struct")
	}
	f := itemValue.Elem().FieldByName(name)
	if !f.IsValid() {
		return reflect.Value{}, fmt.Errorf("getFieldByName: item should have %v field", name)
	}
	return f, nil
}

type structFieldInfo struct {
	Field reflect.StructField
	Tag   []string
	L     string
	R     string
	Slice bool
}

func getStructFieldInfo(structType reflect.Type, fieldName string, bsonTagRequired bool, joinTagRequired bool) (*structFieldInfo, error) {
	if structType.Kind() == reflect.Pointer {
		structType = structType.Elem()
	}
	if structType.Kind() == reflect.Pointer {
		structType = structType.Elem()
	}
	f, exists := structType.FieldByName(fieldName)
	if !exists {
		return nil, fmt.Errorf("getStructFieldInfo: struct should have %v field", fieldName)
	}
	fieldInfo := structFieldInfo{Field: f}
	if bsonTagRequired {
		bsonTag := strings.Split(f.Tag.Get("bson"), ",")
		if len(bsonTag) == 0 {
			return nil, fmt.Errorf("getStructFieldInfo: field %v should have bson tag", fieldName)
		}
		fieldInfo.Tag = bsonTag
	}
	if joinTagRequired {
		joinTag := strings.Split(f.Tag.Get("join"), ",")
		if len(joinTag) < 2 {
			return nil, fmt.Errorf("getStructFieldInfo: field %v should have join tag", fieldName)
		}
		fieldInfo.L = joinTag[0]
		fieldInfo.R = joinTag[1]
	}
	fieldInfo.Slice = fieldInfo.Field.Type.Kind() == reflect.Slice
	return &fieldInfo, nil
}

// Connect Connect
func Connect(uri string, dbName string) (*mongo.Database, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	db := client.Database(dbName)
	return db, nil
}
