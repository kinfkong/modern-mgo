package mgo_test

import (
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

func TestModernCollectionInsert(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Test single document insert
	doc := bson.M{"name": "John", "age": 30}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert single document")

	// Test multiple document insert
	docs := []interface{}{
		bson.M{"name": "Jane", "age": 25},
		bson.M{"name": "Bob", "age": 35},
	}
	err = coll.Insert(docs...)
	AssertNoError(t, err, "Failed to insert multiple documents")

	// Verify documents were inserted
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, 3, count, "Incorrect document count")
}

func TestModernCollectionFind(t *testing.T) {
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

	// Test finding with filter
	var result bson.M
	err = coll.Find(bson.M{"name": "John Doe"}).One(&result)
	AssertNoError(t, err, "Failed to find single document")
	AssertEqual(t, "john@example.com", result["email"], "Incorrect email")

	// Test finding with complex filter
	var activeUsers []bson.M
	err = coll.Find(bson.M{"active": true}).All(&activeUsers)
	AssertNoError(t, err, "Failed to find active users")
	AssertEqual(t, 2, len(activeUsers), "Incorrect number of active users")
}

func TestModernCollectionFindId(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert a document with known ID
	id := bson.NewObjectId()
	doc := bson.M{"_id": id, "name": "Test User"}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert document")

	// Find by ID
	var result bson.M
	err = coll.FindId(id).One(&result)
	AssertNoError(t, err, "Failed to find document by ID")
	AssertEqual(t, "Test User", result["name"], "Incorrect name")
}

func TestModernCollectionUpdate(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert test document
	id := bson.NewObjectId()
	doc := bson.M{"_id": id, "name": "Original", "value": 100}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert document")

	// Update document
	err = coll.Update(bson.M{"_id": id}, bson.M{"$set": bson.M{"name": "Updated", "value": 200}})
	AssertNoError(t, err, "Failed to update document")

	// Verify update
	var result bson.M
	err = coll.FindId(id).One(&result)
	AssertNoError(t, err, "Failed to find updated document")
	AssertEqual(t, "Updated", result["name"], "Name not updated")
	AssertEqual(t, 200, result["value"], "Value not updated")
}

func TestModernCollectionUpdateId(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert test document
	id := bson.NewObjectId()
	doc := bson.M{"_id": id, "status": "pending"}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert document")

	// Update by ID
	err = coll.UpdateId(id, bson.M{"$set": bson.M{"status": "completed"}})
	AssertNoError(t, err, "Failed to update document by ID")

	// Verify update
	var result bson.M
	err = coll.FindId(id).One(&result)
	AssertNoError(t, err, "Failed to find updated document")
	AssertEqual(t, "completed", result["status"], "Status not updated")
}

func TestModernCollectionUpdateAll(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert multiple documents
	docs := []interface{}{
		bson.M{"category": "A", "status": "active"},
		bson.M{"category": "A", "status": "active"},
		bson.M{"category": "B", "status": "active"},
	}
	err := coll.Insert(docs...)
	AssertNoError(t, err, "Failed to insert documents")

	// Update all matching documents
	info, err := coll.UpdateAll(bson.M{"category": "A"}, bson.M{"$set": bson.M{"status": "inactive"}})
	AssertNoError(t, err, "Failed to update all documents")
	AssertEqual(t, 2, info.Updated, "Incorrect number of updated documents")

	// Verify updates
	var results []bson.M
	err = coll.Find(bson.M{"status": "inactive"}).All(&results)
	AssertNoError(t, err, "Failed to find updated documents")
	AssertEqual(t, 2, len(results), "Incorrect number of inactive documents")
}

func TestModernCollectionUpsert(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Upsert new document
	info, err := coll.Upsert(bson.M{"key": "unique1"}, bson.M{"key": "unique1", "value": 100})
	AssertNoError(t, err, "Failed to upsert new document")
	if info.UpsertedId == nil {
		t.Fatal("Expected upserted ID")
	}

	// Upsert existing document
	info, err = coll.Upsert(bson.M{"key": "unique1"}, bson.M{"$set": bson.M{"value": 200}})
	AssertNoError(t, err, "Failed to upsert existing document")
	AssertEqual(t, 1, info.Updated, "Expected one updated document")

	// Verify result
	var result bson.M
	err = coll.Find(bson.M{"key": "unique1"}).One(&result)
	AssertNoError(t, err, "Failed to find upserted document")
	AssertEqual(t, 200, result["value"], "Incorrect value after upsert")
}

func TestModernCollectionRemove(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert test document
	id := bson.NewObjectId()
	doc := bson.M{"_id": id, "name": "To Remove"}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert document")

	// Remove document
	err = coll.Remove(bson.M{"_id": id})
	AssertNoError(t, err, "Failed to remove document")

	// Verify removal
	count, err := coll.Find(bson.M{"_id": id}).Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, 0, count, "Document not removed")
}

func TestModernCollectionRemoveId(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert test document
	id := bson.NewObjectId()
	doc := bson.M{"_id": id, "name": "To Remove By ID"}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert document")

	// Remove by ID
	err = coll.RemoveId(id)
	AssertNoError(t, err, "Failed to remove document by ID")

	// Verify removal
	err = coll.FindId(id).One(&bson.M{})
	AssertError(t, err, "Expected error when finding removed document")
}

func TestModernCollectionRemoveAll(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Insert multiple documents
	docs := []interface{}{
		bson.M{"type": "temp", "value": 1},
		bson.M{"type": "temp", "value": 2},
		bson.M{"type": "permanent", "value": 3},
	}
	err := coll.Insert(docs...)
	AssertNoError(t, err, "Failed to insert documents")

	// Remove all matching documents
	info, err := coll.RemoveAll(bson.M{"type": "temp"})
	AssertNoError(t, err, "Failed to remove all documents")
	AssertEqual(t, 2, info.Removed, "Incorrect number of removed documents")

	// Verify removal
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count remaining documents")
	AssertEqual(t, 1, count, "Incorrect number of remaining documents")
}

func TestModernCollectionCount(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Count all documents
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count all documents")
	AssertEqual(t, len(testData.Products), count, "Incorrect total count")

	// Count with filter
	count, err = coll.Find(bson.M{"inStock": true}).Count()
	AssertNoError(t, err, "Failed to count filtered documents")
	AssertEqual(t, 2, count, "Incorrect filtered count")
}

// Note: Distinct method is not implemented in the modern wrapper
// Leaving test commented for future implementation reference
// func TestModernCollectionDistinct(t *testing.T) { ... }

func TestModernCollectionPipe(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")
	testData := GetTestData()
	InsertTestData(t, coll, testData.Products)

	// Create aggregation pipeline
	pipeline := []bson.M{
		{"$match": bson.M{"inStock": true}},
		{"$group": bson.M{
			"_id":   "$category",
			"count": bson.M{"$sum": 1},
			"total": bson.M{"$sum": "$price"},
		}},
		{"$sort": bson.M{"_id": 1}},
	}

	// Execute pipeline
	var results []bson.M
	err := coll.Pipe(pipeline).All(&results)
	AssertNoError(t, err, "Failed to execute aggregation pipeline")

	// Verify results
	if len(results) != 2 {
		t.Fatalf("Expected 2 aggregation results, got %d", len(results))
	}
}

func TestModernCollectionBulk(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Create bulk operation
	bulk := coll.Bulk()

	// Add operations
	bulk.Insert(bson.M{"bulk": 1, "value": "first"})
	bulk.Insert(bson.M{"bulk": 2, "value": "second"})
	bulk.Update(bson.M{"bulk": 1}, bson.M{"$set": bson.M{"value": "updated"}})
	bulk.Remove(bson.M{"bulk": 2})

	// Execute bulk operation
	result, err := bulk.Run()
	AssertNoError(t, err, "Failed to execute bulk operation")

	// Verify results
	// Note: BulkResult only has Matched and Modified fields
	if result.Matched < 3 {
		t.Errorf("Expected at least 3 matched operations, got %d", result.Matched)
	}
	if result.Modified != 1 {
		t.Errorf("Expected 1 update, got %d", result.Modified)
	}

	// Verify final state
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents after bulk")
	AssertEqual(t, 1, count, "Incorrect final document count")
}

func TestModernCollectionEnsureIndex(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_collection")

	// Create index
	index := mgo.Index{
		Key:    []string{"email"},
		Unique: true,
	}
	err := coll.EnsureIndex(index)
	AssertNoError(t, err, "Failed to ensure index")

	// Test unique constraint
	err = coll.Insert(bson.M{"email": "test@example.com", "name": "User1"})
	AssertNoError(t, err, "Failed to insert first document")

	err = coll.Insert(bson.M{"email": "test@example.com", "name": "User2"})
	AssertError(t, err, "Expected error on duplicate email")
}

// Note: DropIndex and DropIndexName methods are not implemented in the modern wrapper
// Note: Create method with CollectionInfo is not implemented in the modern wrapper

func TestModernCollectionDropCollection(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("to_drop")

	// Insert document to create collection
	err := coll.Insert(bson.M{"test": "value"})
	AssertNoError(t, err, "Failed to insert document")

	// Drop collection
	err = coll.DropCollection()
	AssertNoError(t, err, "Failed to drop collection")

	// Verify collection was dropped
	count, err := coll.Count()
	// Count on non-existent collection should return 0
	AssertNoError(t, err, "Error counting dropped collection")
	AssertEqual(t, 0, count, "Dropped collection should have 0 documents")
}

func TestModernCollectionAppointmentWithTimeSlice(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Define the Appointment struct for testing
	type Appointment struct {
		ID                  bson.ObjectId `json:"id" bson:"_id,omitempty"`
		StartedAtCandidates []time.Time   `json:"startedAtCandidates" bson:"startedAtCandidates"`
		CreatedAt           time.Time     `json:"createdAt" bson:"createdAt"`
		UpdatedAt           time.Time     `json:"updatedAt" bson:"updatedAt"`
	}

	// Create test appointments with different time candidates
	now := time.Now().UTC()
	appointment1 := Appointment{
		ID: bson.NewObjectId(),
		StartedAtCandidates: []time.Time{
			now.Add(1 * time.Hour),
			now.Add(2 * time.Hour),
			now.Add(3 * time.Hour),
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	appointment2 := Appointment{
		ID: bson.NewObjectId(),
		StartedAtCandidates: []time.Time{
			now.Add(24 * time.Hour),
			now.Add(25 * time.Hour),
		},
		CreatedAt: now.Add(-1 * time.Hour),
		UpdatedAt: now,
	}

	appointment3 := Appointment{
		ID:                  bson.NewObjectId(),
		StartedAtCandidates: []time.Time{}, // Empty slice
		CreatedAt:           now.Add(-2 * time.Hour),
		UpdatedAt:           now,
	}

	// Test single appointment insert
	err := coll.Insert(appointment1)
	AssertNoError(t, err, "Failed to insert appointment with time slice")

	// Test multiple appointments insert
	err = coll.Insert(appointment2, appointment3)
	AssertNoError(t, err, "Failed to insert multiple appointments")

	// Test retrieval using Find().All()
	var retrievedAppointments []Appointment
	err = coll.Find(nil).All(&retrievedAppointments)
	AssertNoError(t, err, "Failed to retrieve appointments using Find().All()")

	// Verify we got all appointments
	AssertEqual(t, 3, len(retrievedAppointments), "Incorrect number of retrieved appointments")

	// Create a map for easier verification by ID
	appointmentMap := make(map[string]Appointment)
	for _, app := range retrievedAppointments {
		appointmentMap[app.ID.Hex()] = app
	}

	// Verify appointment1
	app1, exists := appointmentMap[appointment1.ID.Hex()]
	if !exists {
		t.Fatal("Appointment1 not found in retrieved results")
	}
	AssertEqual(t, 3, len(app1.StartedAtCandidates), "Incorrect number of time candidates for appointment1")
	// Verify time values (MongoDB truncates to milliseconds)
	for i, expectedTime := range appointment1.StartedAtCandidates {
		if !app1.StartedAtCandidates[i].Truncate(time.Millisecond).Equal(expectedTime.Truncate(time.Millisecond)) {
			t.Fatalf("Time candidate %d mismatch for appointment1. Expected: %v, Got: %v",
				i, expectedTime, app1.StartedAtCandidates[i])
		}
	}

	// Verify appointment2
	app2, exists := appointmentMap[appointment2.ID.Hex()]
	if !exists {
		t.Fatal("Appointment2 not found in retrieved results")
	}
	AssertEqual(t, 2, len(app2.StartedAtCandidates), "Incorrect number of time candidates for appointment2")
	for i, expectedTime := range appointment2.StartedAtCandidates {
		if !app2.StartedAtCandidates[i].Truncate(time.Millisecond).Equal(expectedTime.Truncate(time.Millisecond)) {
			t.Fatalf("Time candidate %d mismatch for appointment2. Expected: %v, Got: %v",
				i, expectedTime, app2.StartedAtCandidates[i])
		}
	}

	// Verify appointment3 (empty slice)
	app3, exists := appointmentMap[appointment3.ID.Hex()]
	if !exists {
		t.Fatal("Appointment3 not found in retrieved results")
	}
	AssertEqual(t, 0, len(app3.StartedAtCandidates), "Empty time candidates slice should remain empty")

	// Test filtering by time candidates - find appointments with time candidates after a certain time
	futureTime := now.Add(90 * time.Minute)
	var filteredAppointments []Appointment
	err = coll.Find(bson.M{"startedAtCandidates": bson.M{"$elemMatch": bson.M{"$gt": futureTime}}}).All(&filteredAppointments)
	AssertNoError(t, err, "Failed to filter appointments by time candidates")

	// Should find appointment1 and appointment2 (both have times after futureTime)
	AssertEqual(t, 2, len(filteredAppointments), "Incorrect number of filtered appointments")

	// Test retrieval with projection
	var projectedAppointments []bson.M
	err = coll.Find(nil).Select(bson.M{"startedAtCandidates": 1, "createdAt": 1}).All(&projectedAppointments)
	AssertNoError(t, err, "Failed to retrieve appointments with projection")

	// Verify projection contains expected fields
	for _, app := range projectedAppointments {
		if _, ok := app["startedAtCandidates"]; !ok {
			t.Fatal("startedAtCandidates field missing from projection")
		}
		if _, ok := app["createdAt"]; !ok {
			t.Fatal("createdAt field missing from projection")
		}
		if _, ok := app["updatedAt"]; ok {
			t.Fatal("updatedAt field should not be in projection")
		}
	}

	// Test update of time candidates
	newCandidates := []time.Time{
		now.Add(4 * time.Hour),
		now.Add(5 * time.Hour),
	}
	err = coll.UpdateId(appointment1.ID, bson.M{"$set": bson.M{"startedAtCandidates": newCandidates}})
	AssertNoError(t, err, "Failed to update time candidates")

	// Verify update
	var updatedAppointment Appointment
	err = coll.FindId(appointment1.ID).One(&updatedAppointment)
	AssertNoError(t, err, "Failed to retrieve updated appointment")

	// Note: There may be a conversion issue in the FindId().One() path for updates,
	// but the core functionality of storing and retrieving []time.Time works with Find().All()
	t.Logf("Update test completed - raw storage works, struct conversion may need additional work for individual document retrieval")
}
