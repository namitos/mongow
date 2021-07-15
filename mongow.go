//mongo driver wrapper with useful methods for my purposes
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

func getSliceResult(result interface{}) (*sliceResult, error) {
	sliceP := reflect.ValueOf(result)
	if sliceP.Kind() != reflect.Ptr || sliceP.Elem().Kind() != reflect.Slice {
		return nil, fmt.Errorf("result argument should be a ptr to slice of structs")
	}
	sliceV := sliceP.Elem()
	var elemType reflect.Type
	sliceOfPtrs := false
	if sliceV.Type().Elem().Kind() == reflect.Ptr {
		elemType = sliceV.Type().Elem().Elem()
		sliceOfPtrs = true
	} else {
		elemType = sliceV.Type().Elem()
		sliceOfPtrs = false
	}
	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("result argument should be a ptr to slice of structs")
	}
	return &sliceResult{
		SliceOfPtrs: sliceOfPtrs,
		SliceP:      sliceP,
		SliceV:      sliceV,
		Type:        elemType,
	}, nil
}

func getFieldByName(itemValue reflect.Value, name string) (*reflect.Value, error) {
	if itemValue.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("item should be a ptr to struct")
	}
	if itemValue.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("item should be a ptr to struct")
	}
	f := itemValue.Elem().FieldByName(name)
	if !f.IsValid() {
		return nil, fmt.Errorf("item should have %v field", name)
	}
	return &f, nil
}

type structFieldInfo struct {
	Field reflect.StructField
	Tag   []string
	L     string
	R     string
}

func getStructFieldInfo(sr *sliceResult, fieldName string, bsonTagRequired bool, joinTagRequired bool) (*structFieldInfo, error) {
	f, exists := sr.Type.FieldByName(fieldName)
	if !exists {
		return nil, fmt.Errorf("struct should have %v field", fieldName)
	}
	fieldInfo := structFieldInfo{Field: f}
	if bsonTagRequired {
		bsonTag := strings.Split(f.Tag.Get("bson"), ",")
		if len(bsonTag) == 0 {
			return nil, fmt.Errorf("struct field %v should have bson tag", fieldName)
		}
		fieldInfo.Tag = bsonTag
	}
	if joinTagRequired {
		joinTag := strings.Split(f.Tag.Get("join"), ",")
		if len(joinTag) < 2 {
			return nil, fmt.Errorf("struct field %v should have join tag", fieldName)
		}
		fieldInfo.L = joinTag[0]
		fieldInfo.R = joinTag[1]
	}
	return &fieldInfo, nil
}

//Connect Connect
func Connect(ctx context.Context, uri string, dbName string) (*mongo.Database, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	db := client.Database(dbName)
	return db, nil
}

//NewCollection CollectionWrapper constructor
func NewCollection(db *mongo.Database, collectionName string) *CollectionWrapper {
	return &CollectionWrapper{Collection: db.Collection(collectionName)}
}

//CollectionWrapper mongo collection wrapper
type CollectionWrapper struct {
	Collection *mongo.Collection
}

//Create item in mongo
func (w *CollectionWrapper) Create(ctx context.Context, item interface{}) error {
	f, err := getFieldByName(reflect.ValueOf(item), "ID")
	if err != nil {
		return err
	}
	r, err := w.Collection.InsertOne(ctx, item)
	if err != nil {
		return err
	}
	f.Set(reflect.ValueOf(r.InsertedID))
	return nil
}

//Create items
func (w *CollectionWrapper) CreateMany(ctx context.Context, items []interface{}) ([]interface{}, error) {
	options := &options.InsertManyOptions{}
	options.SetOrdered(false)
	res, err := w.Collection.InsertMany(ctx, items, options)
	if res != nil {
		return res.InsertedIDs, err
	}
	return nil, err
}

//Read item. don't use when update is concurrent. it overwrites full db record.
func (w *CollectionWrapper) Update(ctx context.Context, item interface{}) error {
	f, err := getFieldByName(reflect.ValueOf(item), "ID")
	if err != nil {
		return err
	}
	_, err = w.Collection.UpdateOne(ctx, &bson.M{
		"_id": f.Interface(),
	}, &bson.M{
		"$set": item,
	})
	if err != nil {
		return err
	}
	return nil
}

//Read items to slice of struct
func (w *CollectionWrapper) Read(ctx context.Context, result interface{}, where bson.M, optionItems ...*options.FindOptions) error {
	sr, err := getSliceResult(result)
	if err != nil {
		return err
	}
	i := 0
	var optionsItem *options.FindOptions
	if len(optionItems) > 0 {
		optionsItem = optionItems[0]
	}
	cur, err := w.Collection.Find(ctx, where, optionsItem)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		elemp := reflect.New(sr.Type)
		elempInterface := elemp.Interface()
		err = cur.Decode(elempInterface)
		if err != nil {
			return err
		}
		if sr.SliceOfPtrs {
			sr.SliceV = reflect.Append(sr.SliceV, elemp)
		} else {
			sr.SliceV = reflect.Append(sr.SliceV, elemp.Elem())
		}
		i++
	}
	if err := cur.Err(); err != nil {
		return err
	}
	sr.SliceP.Elem().Set(sr.SliceV.Slice(0, i))
	return nil
}

func (w *CollectionWrapper) DeleteByID(ctx context.Context, ID primitive.ObjectID) error {
	_, err := w.Collection.DeleteOne(ctx, bson.M{
		"_id": ID,
	})
	return err
}

func (w *CollectionWrapper) GetByID(ctx context.Context, result interface{}, ID primitive.ObjectID) error {
	r := w.Collection.FindOne(ctx, bson.M{"_id": ID})
	if r.Err() != nil {
		return r.Err()
	}
	err := r.Decode(result)
	if err != nil {
		return err
	}
	return nil
}
