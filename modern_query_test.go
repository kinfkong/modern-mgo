package mgo_test

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

func TestModernQueryOne(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Users)

	// Test finding one document
	var result bson.M
	err := coll.Find(bson.M{"name": "John Doe"}).One(&result)
	AssertNoError(t, err, "Failed to find one document")
	AssertEqual(t, "john@example.com", result["email"], "Incorrect email")

	// Test not found
	err = coll.Find(bson.M{"name": "Non Existent"}).One(&result)
	AssertError(t, err, "Expected error for non-existent document")
}

func TestModernQueryAll(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Users)

	// Test finding all documents
	var results []bson.M
	err := coll.Find(nil).All(&results)
	AssertNoError(t, err, "Failed to find all documents")
	AssertEqual(t, len(testData.Users), len(results), "Incorrect number of results")
}

func TestModernQueryIter(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Users)

	// Test iteration
	iter := coll.Find(nil).Iter()
	var result bson.M
	count := 0
	for iter.Next(&result) {
		count++
	}
	err := iter.Close()
	AssertNoError(t, err, "Failed to close iterator")
	AssertEqual(t, len(testData.Users), count, "Incorrect number of iterated documents")
}

func TestModernQuerySort(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Users)

	// Test sorting ascending
	var results []bson.M
	err := coll.Find(nil).Sort("age").All(&results)
	AssertNoError(t, err, "Failed to sort ascending")

	// Verify order
	for i := 1; i < len(results); i++ {
		prevAge := results[i-1]["age"].(int)
		currAge := results[i]["age"].(int)
		if prevAge > currAge {
			t.Fatal("Results not sorted in ascending order")
		}
	}

	// Test sorting descending
	err = coll.Find(nil).Sort("-age").All(&results)
	AssertNoError(t, err, "Failed to sort descending")

	// Verify order
	for i := 1; i < len(results); i++ {
		prevAge := results[i-1]["age"].(int)
		currAge := results[i]["age"].(int)
		if prevAge < currAge {
			t.Fatal("Results not sorted in descending order")
		}
	}
}

func TestModernQueryLimit(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Test limit
	var results []bson.M
	err := coll.Find(nil).Limit(2).All(&results)
	AssertNoError(t, err, "Failed to apply limit")
	AssertEqual(t, 2, len(results), "Incorrect number of limited results")
}

func TestModernQuerySkip(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Test skip
	var results []bson.M
	err := coll.Find(nil).Skip(1).All(&results)
	AssertNoError(t, err, "Failed to apply skip")
	AssertEqual(t, len(testData.Products)-1, len(results), "Incorrect number of results after skip")
}

func TestModernQuerySelect(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	doc := bson.M{
		"_id":    bson.NewObjectId(),
		"name":   "Test",
		"email":  "test@example.com",
		"age":    30,
		"active": true,
	}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert document")

	// Test projection
	var result bson.M
	err = coll.Find(nil).Select(bson.M{"name": 1, "email": 1}).One(&result)
	AssertNoError(t, err, "Failed to apply projection")

	// Verify fields
	if _, ok := result["name"]; !ok {
		t.Fatal("name field missing from projection")
	}
	if _, ok := result["email"]; !ok {
		t.Fatal("email field missing from projection")
	}
	if _, ok := result["age"]; ok {
		t.Fatal("age field should not be in projection")
	}
	if _, ok := result["active"]; ok {
		t.Fatal("active field should not be in projection")
	}
}

func TestModernQueryCount(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Users)

	// Count all
	count, err := coll.Find(nil).Count()
	AssertNoError(t, err, "Failed to count all documents")
	AssertEqual(t, len(testData.Users), count, "Incorrect total count")

	// Count with filter
	count, err = coll.Find(bson.M{"active": true}).Count()
	AssertNoError(t, err, "Failed to count filtered documents")
	AssertEqual(t, 2, count, "Incorrect filtered count")
}

// Note: Explain, Hint, Batch, and SetMaxTime methods are not implemented in the modern wrapper

func TestModernQueryApply(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert initial document
	id := bson.NewObjectId()
	err := coll.Insert(bson.M{"_id": id, "counter": 0})
	AssertNoError(t, err, "Failed to insert document")

	// Test findAndModify with update
	change := mgo.Change{
		Update:    bson.M{"$inc": bson.M{"counter": 1}},
		ReturnNew: true,
	}
	var result bson.M
	info, err := coll.Find(bson.M{"_id": id}).Apply(change, &result)
	AssertNoError(t, err, "Failed to apply change")
	AssertEqual(t, 1, info.Updated, "Expected one document updated")
	AssertEqual(t, 1, result["counter"], "Counter not incremented")

	// Test findAndModify with remove
	change = mgo.Change{
		Remove: true,
	}
	info, err = coll.Find(bson.M{"_id": id}).Apply(change, &result)
	AssertNoError(t, err, "Failed to apply remove")
	AssertEqual(t, 1, info.Removed, "Expected one document removed")

	// Test upsert
	newId := bson.NewObjectId()
	change = mgo.Change{
		Update: bson.M{"$set": bson.M{"value": "new"}},
		Upsert: true,
	}
	info, err = coll.Find(bson.M{"_id": newId}).Apply(change, &result)
	AssertNoError(t, err, "Failed to apply upsert")
	if info.UpsertedId == nil {
		t.Fatal("Expected upserted ID")
	}
}

func TestModernQueryComplexChaining(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert test data
	for i := 0; i < 20; i++ {
		err := coll.Insert(bson.M{
			"index":    i,
			"category": i % 3, // 0, 1, or 2
			"value":    i * 10,
		})
		AssertNoError(t, err, "Failed to insert document")
	}

	// Test complex query with chaining
	var results []bson.M
	err := coll.Find(bson.M{"category": 1}).
		Sort("-value").
		Skip(1).
		Limit(3).
		Select(bson.M{"index": 1, "value": 1}).
		All(&results)

	AssertNoError(t, err, "Failed to execute complex query")
	AssertEqual(t, 3, len(results), "Incorrect number of results")

	// Verify sorting (descending)
	for i := 1; i < len(results); i++ {
		prevValue := results[i-1]["value"].(int)
		currValue := results[i]["value"].(int)
		if prevValue < currValue {
			t.Fatal("Results not sorted correctly")
		}
	}
}
