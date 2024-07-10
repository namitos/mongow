package mongow

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type testType1 struct {
	ID      primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Created int64              `bson:"created,omitempty" json:"created,omitempty"`
	A       string             `bson:"a,omitempty" json:"a,omitempty"`

	T2Items []*testType2 `bson:"-" json:"t2Items,omitempty" join:"A,B"`
}

type testType2 struct {
	ID      primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Created int64              `bson:"created,omitempty" json:"created,omitempty"`
	B       string             `bson:"b,omitempty" json:"b,omitempty"`
}

func TestAll(t *testing.T) {
	db, err := Connect(context.Background(), "mongodb://127.0.0.1:27017", "test")
	if err != nil {
		t.Error(err)
		return
	}

	collection1 := NewCollectionG[*testType1](db, "testCollection1")
	tInstance1 := &testType1{
		Created: 123,
		A:       "aaa",
	}
	err = collection1.Create(context.Background(), tInstance1)
	if err != nil {
		t.Error(err)
		return
	}
	if tInstance1.ID.IsZero() {
		t.Error("tInstance1.ID should not be zero")
	}

	collection2 := NewCollectionG[*testType2](db, "testCollection2")
	tInstance2 := &testType2{
		Created: 123,
		B:       "aaa",
	}
	err = collection2.Create(context.Background(), tInstance2)
	if err != nil {
		t.Error(err)
		return
	}

	items1, err := collection1.Read(context.Background(), nil)
	if err != nil {
		t.Error(err)
		return
	}
	joinedItems, err := JoinG(context.Background(), &items1, "T2Items", collection2)
	if err != nil {
		t.Error(err)
		return
	}
	if len(joinedItems) == 0 {
		t.Error("joinedItems should not be empty")
	}
}
