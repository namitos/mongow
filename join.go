package mongow

import (
	"context"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
)

type JoinInput struct {
	Context     context.Context
	Src         interface{}
	JoinedItems interface{}
	Field       string
	Collection  *CollectionWrapper
}

//Join makes join to selected struct field according to its "join" tag
func Join(input JoinInput) error {
	if input.Context == nil {
		input.Context = context.Background()
	}
	if input.Src == nil {
		return fmt.Errorf("Src required")
	}
	if input.JoinedItems == nil {
		return fmt.Errorf("JoinedItems required")
	}
	if input.Collection == nil {
		return fmt.Errorf("Collection required")
	}
	if input.Field == "" {
		return fmt.Errorf("Field required")
	}

	asKey := input.Field

	lItems, err := getSliceResult(input.Src)
	if err != nil {
		return err
	}

	asFieldInfo, err := getStructFieldInfo(lItems, asKey, false, true)
	if err != nil {
		return err
	}
	asSlice := asFieldInfo.Field.Type.Kind() == reflect.Slice
	lKey := asFieldInfo.L
	rKey := asFieldInfo.R

	lFieldInfo, err := getStructFieldInfo(lItems, lKey, false, false)
	if err != nil {
		return err
	}
	lSlice := lFieldInfo.Field.Type.Kind() == reflect.Slice

	joinedItemsSR, err := getSliceResult(input.JoinedItems)
	if err != nil {
		return err
	}
	rFieldInfo, err := getStructFieldInfo(joinedItemsSR, rKey, true, false)
	if err != nil {
		return err
	}
	rSlice := rFieldInfo.Field.Type.Kind() == reflect.Slice

	//getting all keys
	var lKeys []interface{}
	for i := 0; i < lItems.SliceV.Len(); i++ {
		item := lItems.SliceV.Index(i)
		itemField, err := getFieldByName(item, lKey)
		if err != nil {
			return err
		}
		if lSlice {
			for i := 0; i < itemField.Len(); i++ {
				lKeys = append(lKeys, itemField.Index(i).Interface())
			}
		} else {
			lKeys = append(lKeys, itemField.Interface())
		}
	}

	//reading
	if len(lKeys) == 0 {
		return nil
	}
	where := bson.M{}
	where[rFieldInfo.Tag[0]] = bson.M{"$in": lKeys}
	if err := input.Collection.Read(input.Context, input.JoinedItems, where); err != nil {
		return err
	}

	//indexing read result
	joinedItemsK := map[interface{}][]reflect.Value{}
	for i := 0; i < joinedItemsSR.SliceV.Len(); i++ {
		item := joinedItemsSR.SliceV.Index(i)
		itemFieldKeyR, err := getFieldByName(item, rKey)
		if err != nil {
			return err
		}
		if rSlice {
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
	for i := 0; i < lItems.SliceV.Len(); i++ {
		item := lItems.SliceV.Index(i)
		asField, err := getFieldByName(item, asKey)
		if err != nil {
			return err
		}
		lField, err := getFieldByName(item, lKey)
		if err != nil {
			return err
		}

		var lKeys []interface{}
		if lSlice {
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
		if asSlice {
			itemsToPush = reflect.MakeSlice(asFieldInfo.Field.Type, 0, 0)
		}
		for _, lKey := range lKeys {
			values := joinedItemsK[lKey]
			joinedLen := len(values)
			if values != nil && joinedLen > 0 {
				if asSlice {
					for _, v := range values {
						itemsToPush = reflect.Append(itemsToPush, v)
					}
				} else {
					asField.Set(values[0])
				}
			}
		}
		if asSlice {
			asField.Set(itemsToPush)
		}
	}
	return nil
}
