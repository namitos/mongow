package mongow

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
)

// NewCollectionG Collection constructor
func NewCollectionG[T any](db *mongo.Database, collectionName string) *Collection[T] {
	return &Collection[T]{db.Collection(collectionName)}
}

// Collection mongo collection wrapper
type Collection[T any] struct{ *mongo.Collection }

// Create InsertOne wrapper that sets ID field after insert
func (w *Collection[T]) Create(ctx context.Context, item T) error {
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
func (w *Collection[T]) Update(ctx context.Context, item T) error {
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
func (w *Collection[T]) Read(ctx context.Context, where bson.M, optionItems ...*options.FindOptions) ([]T, error) {
	cur, err := w.Find(ctx, where, optionItems...)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]T, 0)
	err = cur.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (w *Collection[T]) DeleteByID(ctx context.Context, ID primitive.ObjectID) error {
	_, err := w.DeleteOne(ctx, bson.M{"_id": ID})
	return err
}

func (w *Collection[T]) GetByID(ctx context.Context, ID primitive.ObjectID, result T) error {
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

// JoinG makes join to selected struct field according to its "join" tag
func JoinG[SrcType, JoinedType any](ctx context.Context, src *[]SrcType, asKey string, collection *Collection[JoinedType]) ([]JoinedType, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	lItem := new(SrcType)
	lItemType := reflect.TypeOf(lItem)

	asFieldInfo, err := getStructFieldInfo(lItemType, asKey, false, false)
	if err != nil {
		return nil, err
	}
	lFieldInfo, err := getStructFieldInfo(lItemType, asFieldInfo.L, false, false)
	if err != nil {
		return nil, err
	}

	rItem := new(JoinedType)
	rFieldInfo, err := getStructFieldInfo(reflect.TypeOf(rItem), asFieldInfo.R, true, false)
	if err != nil {
		return nil, err
	}

	//getting all keys
	var lKeys []any
	for _, item := range *src {
		itemField, err := getFieldByName(reflect.ValueOf(item), asFieldInfo.L)
		if err != nil {
			return nil, err
		}
		if lFieldInfo.Slice {
			for i := 0; i < itemField.Len(); i++ {
				lKeys = append(lKeys, itemField.Index(i).Interface())
			}
		} else {
			lKeys = append(lKeys, itemField.Interface())
		}
	}

	//reading
	if len(lKeys) == 0 {
		return nil, nil
	}
	where := bson.M{}
	where[rFieldInfo.Tag[0]] = bson.M{"$in": lKeys}
	joinedItems, err := collection.Read(ctx, where)
	if err != nil {
		return nil, err
	}
	//indexing read result
	joinedItemsK := map[any][]reflect.Value{}
	for _, joinedItem := range joinedItems {
		item := reflect.ValueOf(joinedItem)
		itemFieldKeyR, err := getFieldByName(item, asFieldInfo.R)
		if err != nil {
			return joinedItems, err
		}
		if rFieldInfo.Slice {
			for i := 0; i < itemFieldKeyR.Len(); i++ {
				key := itemFieldKeyR.Index(i).Interface()
				if joinedItemsK[key] == nil {
					joinedItemsK[key] = []reflect.Value{}
				}
				joinedItemsK[key] = append(joinedItemsK[key], item)
			}
		} else {
			key := itemFieldKeyR.Interface()
			if joinedItemsK[key] == nil {
				joinedItemsK[key] = []reflect.Value{}
			}
			joinedItemsK[key] = append(joinedItemsK[key], item)
		}
	}

	//joining
	for _, item1 := range *src {
		item := reflect.ValueOf(item1)
		asField, err := getFieldByName(item, asKey)
		if err != nil {
			return joinedItems, err
		}
		lField, err := getFieldByName(item, asFieldInfo.L)
		if err != nil {
			return joinedItems, err
		}

		var lKeys []any
		if lFieldInfo.Slice {
			for i := 0; i < lField.Len(); i++ {
				lKeys = append(lKeys, lField.Index(i).Interface())
			}
		} else {
			lKeys = append(lKeys, lField.Interface())
		}
		if len(lKeys) == 0 {
			continue
		}
		var itemsToPush reflect.Value
		if asFieldInfo.Slice {
			itemsToPush = reflect.MakeSlice(asFieldInfo.Field.Type, 0, 0)
		}
		for _, lKey := range lKeys {
			values := joinedItemsK[lKey]
			joinedLen := len(values)
			if values != nil && joinedLen > 0 {
				if asFieldInfo.Slice {
					for _, v := range values {
						itemsToPush = reflect.Append(itemsToPush, v)
					}
				} else {
					asField.Set(values[0])
				}
			}
		}
		if asFieldInfo.Slice {
			asField.Set(itemsToPush)
		}
	}
	return joinedItems, nil
}
