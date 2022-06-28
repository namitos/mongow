package mongow

import (
	"log"
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

func tryObjectID(key string, parentKeys []string, value interface{}) (*primitive.ObjectID, error) {
	if !(keyLooksLikeID(key) || anyKeyLooksLikeID(parentKeys)) {
		return nil, nil
	}
	v, ok := value.(string)
	if ok && len(v) == 24 {
		ID, err := primitive.ObjectIDFromHex(v)
		if err != nil {
			return nil, err
		}
		return &ID, nil
	}
	return nil, nil
}

//PrepareBsonWhereFromInput recursive function that prepares ObjectID from suitable map fields
func PrepareBsonWhereFromInput(input map[string]interface{}, parentKeys ...string) {
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
			vMap, ok := value.(map[string]interface{})
			if ok {
				PrepareBsonWhereFromInput(vMap, key)
				input[key] = vMap
				continue
			}
		}
		//slice
		{
			vSlice, ok := value.([]interface{})
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
						vMap, ok := item.(map[string]interface{})
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
