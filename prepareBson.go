package mongow

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func keyLooksLikeID(key string) bool {
	return key == "_id" || strings.HasSuffix(key, "_id") || strings.HasSuffix(key, "ID") || strings.HasSuffix(key, "IDs")
}
func anyKeyLooksLikeID(keys []string) bool {
	for _, key := range keys {
		if keyLooksLikeID(key) {
			return true
		}
	}
	return false
}

func tryObjectID(key string, parentKeys []string, value any) (*primitive.ObjectID, error) {
	if !(keyLooksLikeID(key) || anyKeyLooksLikeID(parentKeys)) {
		return nil, nil
	}
	v, ok := value.(string)
	if ok && len(v) == 24 {
		ID, err := primitive.ObjectIDFromHex(v)
		if err != nil {
			return nil, fmt.Errorf("tryObjectID:%v %v; %w", reflect.TypeOf(value), value, err)
		}
		return &ID, nil
	}
	return nil, nil
}

// PrepareBsonWhereFromInput recursive function that prepares ObjectID from suitable map fields
func PrepareBsonWhereFromInput(input map[string]any, parentKeys ...string) {
	if input == nil {
		return
	}
	for key, value := range input {
		//string
		{
			ID, err := tryObjectID(key, parentKeys, value)
			if ID != nil {
				input[key] = *ID
				continue
			}
			if err != nil {
				log.Println(err)
			}
		}
		//map
		{
			vMap, ok := value.(map[string]any)
			if ok {
				PrepareBsonWhereFromInput(vMap, key)
				input[key] = vMap
				continue
			}
		}
		//slice
		{
			vSlice, ok := value.([]any)
			if ok {
				for i, item := range vSlice {
					//string
					{
						ID, err := tryObjectID(key, parentKeys, item)
						if ID != nil {
							vSlice[i] = *ID
							continue
						}
						if err != nil {
							log.Println(err)
						}
					}
					//map
					{
						vMap, ok := item.(map[string]any)
						if ok {
							PrepareBsonWhereFromInput(vMap, key)
							continue
						}
					}
				}
				input[key] = vSlice
				continue
			}
		}
	}
}
