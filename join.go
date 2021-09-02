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
func Join(input *JoinInput) error {
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

	as := input.Field

	srcSR, err := getSliceResult(input.Src)
	if err != nil {
		return err
	}
	asFieldInfo, err := getStructFieldInfo(srcSR, as, false, true)
	if err != nil {
		return err
	}
	l := asFieldInfo.L
	r := asFieldInfo.R
	_, err = getStructFieldInfo(srcSR, l, false, false)
	if err != nil {
		return err
	}

	joinedItemsSR, err := getSliceResult(input.JoinedItems)
	if err != nil {
		return err
	}
	rFieldInfo, err := getStructFieldInfo(joinedItemsSR, r, true, false)
	if err != nil {
		return err
	}

	in := []interface{}{}
	for i := 0; i < srcSR.SliceV.Len(); i++ {
		item := srcSR.SliceV.Index(i)
		itemField, err := getFieldByName(item, l)
		if err != nil {
			return err
		}
		if itemField.Kind() == reflect.Slice {
			for i := 0; i < itemField.Len(); i++ {
				in = append(in, itemField.Index(i).Interface())
			}
		} else {
			in = append(in, itemField.Interface())
		}
	}

	where := bson.M{}
	where[rFieldInfo.Tag[0]] = bson.M{"$in": in}

	if err := input.Collection.Read(input.Context, input.JoinedItems, where); err != nil {
		return err
	}

	//indexing read result
	joinedItemsK := map[interface{}][]reflect.Value{}
	for i := 0; i < joinedItemsSR.SliceV.Len(); i++ {
		item := joinedItemsSR.SliceV.Index(i)
		itemFieldKeyR, err := getFieldByName(item, r)
		if err != nil {
			return err
		}
		if itemFieldKeyR.Kind() == reflect.Slice {
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

	//
	asSlice := asFieldInfo.Field.Type.Kind() == reflect.Slice
	for i := 0; i < srcSR.SliceV.Len(); i++ {
		item := srcSR.SliceV.Index(i)
		itemFieldToJoin, err := getFieldByName(item, as)
		if err != nil {
			return err
		}
		itemFieldKeyL, err := getFieldByName(item, l)
		if err != nil {
			return err
		}

		in := []interface{}{}
		if itemFieldKeyL.Kind() == reflect.Slice {
			for i := 0; i < itemFieldKeyL.Len(); i++ {
				in = append(in, itemFieldKeyL.Index(i).Interface())
			}
		} else {
			in = append(in, itemFieldKeyL.Interface())
		}
		var itemsToPush reflect.Value
		if asSlice {
			itemsToPush = reflect.MakeSlice(asFieldInfo.Field.Type, 0, 0)
		}
		for _, key := range in {
			values := joinedItemsK[key]
			jLen := len(values)
			if values != nil && jLen > 0 {
				if asSlice {
					for _, v := range values {
						itemsToPush = reflect.Append(itemsToPush, v)
					}
				} else {
					itemFieldToJoin.Set(values[0])
				}
			}
		}
		if asSlice {
			itemFieldToJoin.Set(itemsToPush)
		}
	}
	return nil
}
