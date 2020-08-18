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
		in = append(in, itemField.Interface())
	}
	where := bson.M{}
	where[rFieldInfo.Tag[0]] = bson.M{"$in": in}

	if err := input.Collection.Read(input.Context, input.JoinedItems, where); err != nil {
		return err
	}

	joinedItemsK := map[interface{}][]reflect.Value{}
	for i := 0; i < joinedItemsSR.SliceV.Len(); i++ {
		item := joinedItemsSR.SliceV.Index(i)
		itemFieldKey, err := getFieldByName(item, r)
		if err != nil {
			return err
		}
		if joinedItemsK[itemFieldKey.Interface()] == nil {
			joinedItemsK[itemFieldKey.Interface()] = []reflect.Value{}
		}
		joinedItemsK[itemFieldKey.Interface()] = append(joinedItemsK[itemFieldKey.Interface()], item)
	}

	for i := 0; i < srcSR.SliceV.Len(); i++ {
		item := srcSR.SliceV.Index(i)
		itemFieldToJoin, err := getFieldByName(item, as)
		if err != nil {
			return err
		}
		itemFieldKey, err := getFieldByName(item, l)
		if err != nil {
			return err
		}
		jLen := len(joinedItemsK[itemFieldKey.Interface()])
		if joinedItemsK[itemFieldKey.Interface()] != nil && jLen > 0 {
			if asFieldInfo.Field.Type.Kind() == reflect.Slice {
				itemsToPush := reflect.MakeSlice(asFieldInfo.Field.Type, 0, jLen)
				for _, v := range joinedItemsK[itemFieldKey.Interface()] {
					itemsToPush = reflect.Append(itemsToPush, v)
				}
				itemFieldToJoin.Set(itemsToPush)
			} else {
				itemFieldToJoin.Set(joinedItemsK[itemFieldKey.Interface()][0])
			}
		}
	}
	return nil
}
