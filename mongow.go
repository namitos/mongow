// Package mongow mongo driver wrapper with useful methods for my purposes
package mongow

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type sliceResult struct {
	SliceOfPtrs bool
	SliceP      reflect.Value //pointer to slice
	SliceV      reflect.Value //slice
	Type        reflect.Type
}

func getSliceResult(result any) (*sliceResult, error) {
	sliceP := reflect.ValueOf(result)
	if sliceP.Kind() != reflect.Ptr || sliceP.Elem().Kind() != reflect.Slice {
		return nil, fmt.Errorf("getSliceResult: result argument should be a ptr to slice of structs")
	}
	sliceV := sliceP.Elem()
	elemType := sliceV.Type().Elem()
	sliceOfPtrs := false
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		sliceOfPtrs = true
	}
	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("getSliceResult: result argument should be a ptr to slice of structs")
	}
	return &sliceResult{
		SliceOfPtrs: sliceOfPtrs,
		SliceP:      sliceP,
		SliceV:      sliceV,
		Type:        elemType,
	}, nil
}

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

func getSliceStructFieldInfo(sr *sliceResult, fieldName string, bsonTagRequired bool, joinTagRequired bool) (*structFieldInfo, error) {
	return getStructFieldInfo(sr.Type, fieldName, bsonTagRequired, joinTagRequired)
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
func Connect(ctx context.Context, uri string, dbName string) (*mongo.Database, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	db := client.Database(dbName)
	return db, nil
}

// NewCollection CollectionWrapper constructor
func NewCollection(db *mongo.Database, collectionName string) *CollectionWrapper {
	return &CollectionWrapper{db.Collection(collectionName)}
}

// CollectionWrapper mongo collection wrapper
type CollectionWrapper struct{ *mongo.Collection }

// Create InsertOne wrapper that sets ID field after insert
func (w *CollectionWrapper) Create(ctx context.Context, item any) error {
	f, err := getFieldByName(reflect.ValueOf(item), "ID")
	if err != nil {
		return err
	}
	r, err := w.InsertOne(ctx, item)
	if err != nil {
		return err
	}
	f.Set(reflect.ValueOf(r.InsertedID))
	return nil
}

// Update UpdateOne wrapper that uses $set. don't use when update is concurrent. it overwrites full db record.
func (w *CollectionWrapper) Update(ctx context.Context, item any) error {
	f, err := getFieldByName(reflect.ValueOf(item), "ID")
	if err != nil {
		return err
	}
	_, err = w.UpdateByID(ctx, f.Interface(), &bson.M{"$set": item})
	if err != nil {
		return err
	}
	return nil
}

// Read Find wrapper that extracts items to slice of struct
func (w *CollectionWrapper) Read(ctx context.Context, result any, where bson.M, optionItems ...*options.FindOptions) error {
	sr, err := getSliceResult(result)
	if err != nil {
		return err
	}
	cur, err := w.Find(ctx, where, optionItems...)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	err = cur.All(ctx, sr.SliceP.Interface())
	if err != nil {
		return err
	}
	return nil
}

func (w *CollectionWrapper) DeleteByID(ctx context.Context, ID primitive.ObjectID) error {
	_, err := w.DeleteOne(ctx, bson.M{"_id": ID})
	return err
}

func (w *CollectionWrapper) GetByID(ctx context.Context, ID primitive.ObjectID, result any) error {
	r := w.FindOne(ctx, bson.M{"_id": ID})
	if r.Err() != nil {
		return r.Err()
	}
	err := r.Decode(result)
	if err != nil {
		return err
	}
	return nil
}
