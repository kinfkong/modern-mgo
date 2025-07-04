package mgo_test

import (
	"testing"

	"github.com/globalsign/mgo/bson"
)

func TestModernAggregationBasic(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Basic aggregation pipeline
	pipeline := []bson.M{
		{"$match": bson.M{"inStock": true}},
		{"$group": bson.M{
			"_id":        "$category",
			"totalPrice": bson.M{"$sum": "$price"},
			"count":      bson.M{"$sum": 1},
		}},
	}

	var results []bson.M
	err := coll.Pipe(pipeline).All(&results)
	AssertNoError(t, err, "Failed to execute aggregation pipeline")

	// Verify we got results
	if len(results) < 1 {
		t.Fatal("Expected aggregation results")
	}
}

func TestModernAggregationOne(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Pipeline that returns single result
	pipeline := []bson.M{
		{"$group": bson.M{
			"_id":        nil,
			"totalPrice": bson.M{"$sum": "$price"},
			"avgPrice":   bson.M{"$avg": "$price"},
			"count":      bson.M{"$sum": 1},
		}},
	}

	var result bson.M
	err := coll.Pipe(pipeline).One(&result)
	AssertNoError(t, err, "Failed to execute aggregation pipeline")

	// Verify result
	if result["count"] != len(testData.Products) {
		t.Fatalf("Expected count %d, got %v", len(testData.Products), result["count"])
	}
}

func TestModernAggregationIter(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Pipeline with multiple results
	pipeline := []bson.M{
		{"$sort": bson.M{"price": 1}},
		{"$project": bson.M{
			"name":  1,
			"price": 1,
		}},
	}

	iter := coll.Pipe(pipeline).Iter()
	defer iter.Close()

	var result bson.M
	count := 0
	for iter.Next(&result) {
		count++
		// Verify fields
		if result["name"] == nil || result["price"] == nil {
			t.Fatal("Missing expected fields in result")
		}
	}

	AssertEqual(t, len(testData.Products), count, "Incorrect number of aggregation results")
}

func TestModernAggregationAllowDiskUse(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert many documents for testing
	for i := 0; i < 100; i++ {
		err := coll.Insert(bson.M{
			"index": i,
			"value": i * 10,
		})
		AssertNoError(t, err, "Failed to insert document")
	}

	// Pipeline that might need disk use for large datasets
	pipeline := []bson.M{
		{"$sort": bson.M{"value": -1}},
		{"$group": bson.M{
			"_id":   bson.M{"$mod": []interface{}{"$index", 10}},
			"total": bson.M{"$sum": "$value"},
			"docs":  bson.M{"$push": "$$ROOT"},
		}},
		{"$sort": bson.M{"_id": 1}},
	}

	var results []bson.M
	err := coll.Pipe(pipeline).AllowDiskUse().All(&results)
	AssertNoError(t, err, "Failed to execute aggregation with disk use")

	// Should have 10 groups (0-9)
	AssertEqual(t, 10, len(results), "Incorrect number of groups")
}

func TestModernAggregationBatch(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert many documents
	numDocs := 100
	for i := 0; i < numDocs; i++ {
		err := coll.Insert(bson.M{"index": i})
		AssertNoError(t, err, "Failed to insert document")
	}

	// Simple pipeline
	pipeline := []bson.M{
		{"$sort": bson.M{"index": 1}},
	}

	// Test with batch size
	iter := coll.Pipe(pipeline).Batch(10).Iter()
	defer iter.Close()

	var result bson.M
	count := 0
	for iter.Next(&result) {
		count++
	}

	AssertEqual(t, numDocs, count, "Incorrect number of results with batching")
}

func TestModernAggregationComplexPipeline(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert test data
	orders := []bson.M{
		{
			"customer": "Alice",
			"items": []bson.M{
				{"product": "A", "quantity": 2, "price": 10.0},
				{"product": "B", "quantity": 1, "price": 20.0},
			},
			"date": "2024-01-01",
		},
		{
			"customer": "Bob",
			"items": []bson.M{
				{"product": "A", "quantity": 1, "price": 10.0},
				{"product": "C", "quantity": 3, "price": 15.0},
			},
			"date": "2024-01-02",
		},
		{
			"customer": "Alice",
			"items": []bson.M{
				{"product": "B", "quantity": 2, "price": 20.0},
			},
			"date": "2024-01-03",
		},
	}

	for _, order := range orders {
		err := coll.Insert(order)
		AssertNoError(t, err, "Failed to insert order")
	}

	// Complex pipeline with multiple stages
	pipeline := []bson.M{
		// Unwind items array
		{"$unwind": "$items"},
		// Calculate item total
		{"$addFields": bson.M{
			"items.total": bson.M{
				"$multiply": []interface{}{"$items.quantity", "$items.price"},
			},
		}},
		// Group by customer
		{"$group": bson.M{
			"_id":        "$customer",
			"totalSpent": bson.M{"$sum": "$items.total"},
			"orderCount": bson.M{"$addToSet": "$date"},
			"products":   bson.M{"$addToSet": "$items.product"},
			"totalItems": bson.M{"$sum": "$items.quantity"},
		}},
		// Calculate order count
		{"$addFields": bson.M{
			"orderCount":   bson.M{"$size": "$orderCount"},
			"productCount": bson.M{"$size": "$products"},
		}},
		// Sort by total spent
		{"$sort": bson.M{"totalSpent": -1}},
	}

	var results []bson.M
	err := coll.Pipe(pipeline).All(&results)
	AssertNoError(t, err, "Failed to execute complex aggregation")

	// Verify results
	AssertEqual(t, 2, len(results), "Expected 2 customers")

	// Check first customer (should be Alice with higher total)
	first := results[0]
	if first["_id"] != "Alice" {
		t.Fatalf("Expected Alice as top spender, got %v", first["_id"])
	}
	if first["totalSpent"].(float64) != 80.0 {
		t.Fatalf("Expected total spent 80, got %v", first["totalSpent"])
	}
}

func TestModernAggregationEmptyPipeline(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Users)

	// Empty pipeline should return all documents
	pipeline := []bson.M{}

	var results []bson.M
	err := coll.Pipe(pipeline).All(&results)
	AssertNoError(t, err, "Failed to execute empty pipeline")

	AssertEqual(t, len(testData.Users), len(results), "Empty pipeline should return all documents")
}

func TestModernAggregationNoResults(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Pipeline that filters out all documents
	pipeline := []bson.M{
		{"$match": bson.M{"category": "NonExistent"}},
	}

	var results []bson.M
	err := coll.Pipe(pipeline).All(&results)
	AssertNoError(t, err, "Failed to execute pipeline")

	AssertEqual(t, 0, len(results), "Expected no results")

	// Test with One() on empty result
	var result bson.M
	err = coll.Pipe(pipeline).One(&result)
	AssertError(t, err, "Expected error when no documents match")
}
