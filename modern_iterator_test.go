package mgo_test

import (
	"testing"

	"github.com/globalsign/mgo/bson"
)

func TestModernIteratorNext(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Users)

	// Test iteration
	iter := coll.Find(nil).Iter()
	defer iter.Close()

	var result bson.M
	count := 0
	for iter.Next(&result) {
		count++
		// Verify we got valid data
		if result["name"] == nil {
			t.Fatal("Iterator returned document without name field")
		}
	}

	AssertEqual(t, len(testData.Users), count, "Incorrect number of iterated documents")
}

func TestModernIteratorEmpty(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Test iterator with no documents
	iter := coll.Find(bson.M{"nonexistent": "field"}).Iter()
	defer iter.Close()

	var result bson.M
	hasNext := iter.Next(&result)
	if hasNext {
		t.Fatal("Expected no results from iterator")
	}
}

func TestModernIteratorClose(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Create iterator
	iter := coll.Find(nil).Iter()

	// Read one document
	var result bson.M
	if !iter.Next(&result) {
		t.Fatal("Expected at least one document")
	}

	// Close iterator
	err := iter.Close()
	AssertNoError(t, err, "Failed to close iterator")

	// Verify we can't use iterator after closing
	hasNext := iter.Next(&result)
	if hasNext {
		t.Fatal("Iterator should not return results after closing")
	}
}

func TestModernIteratorAll(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Create iterator and use All
	iter := coll.Find(nil).Iter()

	var results []bson.M
	err := iter.All(&results)
	AssertNoError(t, err, "Failed to get all results from iterator")
	AssertEqual(t, len(testData.Products), len(results), "Incorrect number of results")

	// All method should handle closing internally
}

// Note: Timeout and Err methods are not implemented in the modern wrapper

func TestModernIteratorWithLargeDataset(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert large dataset
	numDocs := 1000
	for i := 0; i < numDocs; i++ {
		err := coll.Insert(bson.M{
			"index": i,
			"value": i * 10,
			"data":  "some test data for document " + string(rune(i)),
		})
		AssertNoError(t, err, "Failed to insert document")
	}

	// Test iteration over large dataset
	iter := coll.Find(nil).Sort("index").Iter()
	defer iter.Close()

	var result bson.M
	count := 0
	lastIndex := -1

	for iter.Next(&result) {
		count++

		// Verify order
		currentIndex := result["index"].(int)
		if currentIndex <= lastIndex {
			t.Fatal("Results not in ascending order")
		}
		lastIndex = currentIndex
	}

	// Verify we got all documents
	AssertEqual(t, numDocs, count, "Incorrect number of iterated documents")
}

func TestModernIteratorPartialIteration(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Users)

	// Create iterator
	iter := coll.Find(nil).Iter()
	defer iter.Close()

	// Read only first document
	var result bson.M
	if !iter.Next(&result) {
		t.Fatal("Expected at least one document")
	}

	// Close without reading all documents
	err := iter.Close()
	AssertNoError(t, err, "Failed to close iterator after partial iteration")
}
