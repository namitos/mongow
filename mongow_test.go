package mongow

import (
	"context"
	"log"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type testType struct {
	ID      primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Created int64              `bson:"created,omitempty" json:"created,omitempty"`
}

func TestCreate(t *testing.T) {
	db, err := Connect(context.Background(), "mongodb://127.0.0.1:27017", "test")
	if err != nil {
		t.Error(err)
	}
	tInstance := &testType{
		Created: 123,
	}
	testCollection := NewCollection(db, "testCollection")
	err = testCollection.Create(context.Background(), tInstance)
	if err != nil {
		t.Error(err)
	}
	log.Println("tInstance.ID", tInstance.ID)
}
