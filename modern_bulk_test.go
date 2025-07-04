package mgo_test

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

func TestModernBulkInsert(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Create bulk operation
	bulk := coll.Bulk()

	// Add insert operations
	bulk.Insert(bson.M{"_id": 1, "name": "Doc1"})
	bulk.Insert(bson.M{"_id": 2, "name": "Doc2"})
	bulk.Insert(bson.M{"_id": 3, "name": "Doc3"})

	// Execute
	result, err := bulk.Run()
	AssertNoError(t, err, "Failed to execute bulk insert")

	// Verify results
	if result.Matched < 0 {
		t.Errorf("Expected non-negative matched count, got %d", result.Matched)
	}

	// Verify documents were inserted
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, 3, count, "Incorrect number of documents after bulk insert")
}

func TestModernBulkUpdate(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert initial documents
	docs := []interface{}{
		bson.M{"_id": 1, "status": "pending"},
		bson.M{"_id": 2, "status": "pending"},
		bson.M{"_id": 3, "status": "active"},
	}
	err := coll.Insert(docs...)
	AssertNoError(t, err, "Failed to insert initial documents")

	// Create bulk operation
	bulk := coll.Bulk()

	// Add update operations
	bulk.Update(bson.M{"_id": 1}, bson.M{"$set": bson.M{"status": "completed"}})
	bulk.Update(bson.M{"_id": 2}, bson.M{"$set": bson.M{"status": "completed"}})
	bulk.UpdateAll(bson.M{"status": "pending"}, bson.M{"$set": bson.M{"status": "reviewed"}})

	// Execute
	result, err := bulk.Run()
	AssertNoError(t, err, "Failed to execute bulk update")

	// Verify results
	if result.Modified != 2 {
		t.Errorf("Expected 2 modified documents, got %d", result.Modified)
	}

	// Verify updates
	var doc bson.M
	err = coll.FindId(1).One(&doc)
	AssertNoError(t, err, "Failed to find document")
	AssertEqual(t, "completed", doc["status"], "Document 1 not updated")
}

func TestModernBulkUpsert(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert one document
	err := coll.Insert(bson.M{"_id": 1, "value": 100})
	AssertNoError(t, err, "Failed to insert initial document")

	// Create bulk operation
	bulk := coll.Bulk()

	// Add upsert operations
	bulk.Upsert(bson.M{"_id": 1}, bson.M{"$set": bson.M{"value": 200}}) // Update existing
	bulk.Upsert(bson.M{"_id": 2}, bson.M{"_id": 2, "value": 300})       // Insert new

	// Execute
	result, err := bulk.Run()
	AssertNoError(t, err, "Failed to execute bulk upsert")

	// Verify results
	if result.Modified != 1 {
		t.Errorf("Expected 1 modified document, got %d", result.Modified)
	}

	// Verify final state
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, 2, count, "Incorrect number of documents after upsert")

	// Verify values
	var doc bson.M
	err = coll.FindId(1).One(&doc)
	AssertNoError(t, err, "Failed to find document 1")
	AssertEqual(t, 200, doc["value"], "Document 1 value not updated")

	err = coll.FindId(2).One(&doc)
	AssertNoError(t, err, "Failed to find document 2")
	AssertEqual(t, 300, doc["value"], "Document 2 value incorrect")
}

func TestModernBulkRemove(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert initial documents
	docs := []interface{}{
		bson.M{"_id": 1, "category": "A"},
		bson.M{"_id": 2, "category": "B"},
		bson.M{"_id": 3, "category": "A"},
		bson.M{"_id": 4, "category": "C"},
	}
	err := coll.Insert(docs...)
	AssertNoError(t, err, "Failed to insert initial documents")

	// Create bulk operation
	bulk := coll.Bulk()

	// Add remove operations
	bulk.Remove(bson.M{"_id": 1})
	bulk.RemoveAll(bson.M{"category": "A"})

	// Execute
	_, err = bulk.Run()
	AssertNoError(t, err, "Failed to execute bulk remove")

	// Note: BulkResult doesn't have a Removed field, so we check by counting

	// Verify removals
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, 2, count, "Incorrect number of documents after removal")

	// Verify specific documents were removed
	err = coll.FindId(1).One(&bson.M{})
	AssertError(t, err, "Document 1 should have been removed")

	err = coll.FindId(3).One(&bson.M{})
	AssertError(t, err, "Document 3 should have been removed")
}

func TestModernBulkMixedOperations(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert initial document
	err := coll.Insert(bson.M{"_id": 1, "value": 100})
	AssertNoError(t, err, "Failed to insert initial document")

	// Create bulk operation with mixed operations
	bulk := coll.Bulk()

	// Mix of operations
	bulk.Insert(bson.M{"_id": 2, "value": 200})
	bulk.Update(bson.M{"_id": 1}, bson.M{"$set": bson.M{"value": 150}})
	bulk.Insert(bson.M{"_id": 3, "value": 300})
	bulk.Remove(bson.M{"_id": 2})
	bulk.Upsert(bson.M{"_id": 4}, bson.M{"_id": 4, "value": 400})

	// Execute
	_, err = bulk.Run()
	AssertNoError(t, err, "Failed to execute mixed bulk operations")

	// Verify final state
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, 3, count, "Incorrect number of documents after mixed operations")

	// Verify specific documents
	var doc bson.M

	err = coll.FindId(1).One(&doc)
	AssertNoError(t, err, "Failed to find document 1")
	AssertEqual(t, 150, doc["value"], "Document 1 not updated")

	err = coll.FindId(2).One(&doc)
	AssertError(t, err, "Document 2 should have been removed")

	err = coll.FindId(3).One(&doc)
	AssertNoError(t, err, "Failed to find document 3")
	AssertEqual(t, 300, doc["value"], "Document 3 not inserted")

	err = coll.FindId(4).One(&doc)
	AssertNoError(t, err, "Failed to find document 4")
	AssertEqual(t, 400, doc["value"], "Document 4 not upserted")
}

func TestModernBulkUnordered(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Create unique index
	err := coll.EnsureIndex(mgo.Index{
		Key:    []string{"unique_field"},
		Unique: true,
	})
	AssertNoError(t, err, "Failed to create unique index")

	// Create unordered bulk operation
	bulk := coll.Bulk()
	bulk.Unordered()

	// Add operations (one will fail due to duplicate)
	bulk.Insert(bson.M{"unique_field": "value1"})
	bulk.Insert(bson.M{"unique_field": "value1"}) // This will fail
	bulk.Insert(bson.M{"unique_field": "value2"})
	bulk.Insert(bson.M{"unique_field": "value3"})

	// Execute - expect partial success with unordered
	_, err = bulk.Run()
	// Error is expected due to duplicate, but other operations should succeed
	if err == nil {
		t.Fatal("Expected error due to duplicate key")
	}

	// Verify that successful operations were executed
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	if count < 3 {
		t.Errorf("Expected at least 3 documents with unordered bulk, got %d", count)
	}
}

func TestModernBulkEmptyOperations(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Create bulk operation with no operations
	bulk := coll.Bulk()

	// Execute empty bulk
	result, err := bulk.Run()
	// Some implementations might error on empty bulk, others might succeed
	if err != nil {
		// If error, it should indicate no operations
		return
	}

	// If no error, result should show no modifications
	if result.Matched != 0 || result.Modified != 0 {
		t.Error("Empty bulk operation should not match or modify any documents")
	}
}

func TestModernBulkLargeOperations(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Create bulk operation with many operations
	bulk := coll.Bulk()

	numOps := 100
	for i := 0; i < numOps; i++ {
		bulk.Insert(bson.M{"_id": i, "value": i * 10})
	}

	// Execute
	result, err := bulk.Run()
	AssertNoError(t, err, "Failed to execute large bulk operation")

	// Verify all documents were inserted
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, numOps, count, "Not all documents were inserted")

	// Create another bulk to update all
	bulk2 := coll.Bulk()
	for i := 0; i < numOps; i++ {
		bulk2.Update(bson.M{"_id": i}, bson.M{"$inc": bson.M{"value": 1}})
	}

	result, err = bulk2.Run()
	AssertNoError(t, err, "Failed to execute bulk update")

	if result.Modified != numOps {
		t.Errorf("Expected %d modified documents, got %d", numOps, result.Modified)
	}
}
