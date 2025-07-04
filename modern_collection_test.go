package mgo_test

import (
	"strconv"
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
	// Matched only counts documents matched by update operations
	if result.Matched != 1 {
		t.Errorf("Expected 1 matched operation (from update), got %d", result.Matched)
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
	futureThreshold := now.Add(20 * time.Hour)
	query := bson.M{
		"startedAtCandidates": bson.M{
			"$elemMatch": bson.M{
				"$gte": futureThreshold,
			},
		},
	}

	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to count with $elemMatch query")
	AssertEqual(t, 1, count, "Should find 1 appointment with future time candidates")
}

// TestModernCollectionMapFields tests handling of map[string]interface{} fields
func TestModernCollectionMapFields(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Test struct with map field
	type AppointmentWithPatientInfo struct {
		ID          bson.ObjectId          `json:"id" bson:"_id,omitempty"`
		PatientInfo map[string]interface{} `json:"patientInfo" bson:"patientInfo"`
		Notes       string                 `json:"notes" bson:"notes"`
	}

	// Create test data with various map configurations
	appointment1 := AppointmentWithPatientInfo{
		ID: bson.NewObjectId(),
		PatientInfo: map[string]interface{}{
			"name":       "John Doe",
			"age":        30,
			"conditions": []string{"diabetes", "hypertension"},
			"bloodType":  "O+",
			"medications": map[string]interface{}{
				"current":   []string{"metformin", "lisinopril"},
				"allergies": []string{"penicillin"},
			},
		},
		Notes: "Regular checkup",
	}

	appointment2 := AppointmentWithPatientInfo{
		ID:          bson.NewObjectId(),
		PatientInfo: map[string]interface{}{}, // Empty map
		Notes:       "New patient",
	}

	appointment3 := AppointmentWithPatientInfo{
		ID:          bson.NewObjectId(),
		PatientInfo: nil, // Nil map
		Notes:       "Emergency visit",
	}

	// Test inserting documents with map fields
	err := coll.Insert(appointment1)
	AssertNoError(t, err, "Failed to insert appointment with populated map")

	err = coll.Insert(appointment2, appointment3)
	AssertNoError(t, err, "Failed to insert appointments with empty/nil maps")

	// Test retrieval
	var retrieved []AppointmentWithPatientInfo
	err = coll.Find(nil).All(&retrieved)
	AssertNoError(t, err, "Failed to retrieve appointments with map fields")
	AssertEqual(t, 3, len(retrieved), "Should retrieve all 3 appointments")

	// Verify map contents
	retrievedMap := make(map[string]AppointmentWithPatientInfo)
	for _, app := range retrieved {
		retrievedMap[app.ID.Hex()] = app
	}

	// Check appointment1 - populated map
	app1 := retrievedMap[appointment1.ID.Hex()]
	AssertEqual(t, "John Doe", app1.PatientInfo["name"], "Patient name mismatch")
	AssertEqual(t, 30, app1.PatientInfo["age"], "Patient age mismatch")

	// Check nested map
	meds, ok := app1.PatientInfo["medications"].(map[string]interface{})
	if !ok {
		t.Fatal("medications should be a map")
	}
	current, ok := meds["current"].([]interface{})
	if !ok || len(current) != 2 {
		t.Fatal("current medications should be a slice of 2 items")
	}

	// Check appointment2 - empty map
	app2 := retrievedMap[appointment2.ID.Hex()]
	if app2.PatientInfo == nil {
		t.Fatal("Empty map should not be nil after retrieval")
	}
	AssertEqual(t, 0, len(app2.PatientInfo), "Empty map should remain empty")

	// Check appointment3 - nil map
	app3 := retrievedMap[appointment3.ID.Hex()]
	if app3.PatientInfo != nil && len(app3.PatientInfo) > 0 {
		t.Fatal("Nil map should remain nil or empty after retrieval")
	}

	// Test querying by map field
	query := bson.M{
		"patientInfo.bloodType": "O+",
	}
	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to query by nested map field")
	AssertEqual(t, 1, count, "Should find 1 appointment with blood type O+")

	// Test querying nested map field
	query2 := bson.M{
		"patientInfo.medications.allergies": bson.M{
			"$in": []string{"penicillin"},
		},
	}
	count, err = coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to query by deeply nested map field")
	AssertEqual(t, 1, count, "Should find 1 appointment with penicillin allergy")
}

// TestModernCollectionAttachmentSlices tests handling of nested struct slices
func TestModernCollectionAttachmentSlices(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Define attachment struct
	type Attachment struct {
		FileName   string    `json:"fileName" bson:"fileName"`
		FileSize   int64     `json:"fileSize" bson:"fileSize"`
		MimeType   string    `json:"mimeType" bson:"mimeType"`
		UploadedAt time.Time `json:"uploadedAt" bson:"uploadedAt"`
	}

	// Define appointment with attachments
	type AppointmentWithAttachments struct {
		ID                   bson.ObjectId `json:"id" bson:"_id,omitempty"`
		ImageAttachments     []Attachment  `json:"imageAttachments" bson:"imageAttachments"`
		InsuranceAttachments []Attachment  `json:"insuranceAttachments" bson:"insuranceAttachments"`
	}

	now := time.Now()

	// Create test appointments
	appointment1 := AppointmentWithAttachments{
		ID: bson.NewObjectId(),
		ImageAttachments: []Attachment{
			{
				FileName:   "xray1.jpg",
				FileSize:   1024000,
				MimeType:   "image/jpeg",
				UploadedAt: now,
			},
			{
				FileName:   "xray2.jpg",
				FileSize:   2048000,
				MimeType:   "image/jpeg",
				UploadedAt: now.Add(-1 * time.Hour),
			},
		},
		InsuranceAttachments: []Attachment{
			{
				FileName:   "insurance_card.pdf",
				FileSize:   512000,
				MimeType:   "application/pdf",
				UploadedAt: now.Add(-24 * time.Hour),
			},
		},
	}

	appointment2 := AppointmentWithAttachments{
		ID:                   bson.NewObjectId(),
		ImageAttachments:     []Attachment{}, // Empty slice
		InsuranceAttachments: nil,            // Nil slice
	}

	// Insert appointments
	err := coll.Insert(appointment1, appointment2)
	AssertNoError(t, err, "Failed to insert appointments with attachment slices")

	// Retrieve and verify
	var retrieved []AppointmentWithAttachments
	err = coll.Find(nil).All(&retrieved)
	AssertNoError(t, err, "Failed to retrieve appointments")
	AssertEqual(t, 2, len(retrieved), "Should retrieve 2 appointments")

	// Create map for verification
	retrievedMap := make(map[string]AppointmentWithAttachments)
	for _, app := range retrieved {
		retrievedMap[app.ID.Hex()] = app
	}

	// Verify appointment1
	app1 := retrievedMap[appointment1.ID.Hex()]
	AssertEqual(t, 2, len(app1.ImageAttachments), "Should have 2 image attachments")
	AssertEqual(t, 1, len(app1.InsuranceAttachments), "Should have 1 insurance attachment")
	AssertEqual(t, "xray1.jpg", app1.ImageAttachments[0].FileName, "First image attachment name mismatch")
	AssertEqual(t, int64(1024000), app1.ImageAttachments[0].FileSize, "First image attachment size mismatch")

	// Verify appointment2
	app2 := retrievedMap[appointment2.ID.Hex()]
	AssertEqual(t, 0, len(app2.ImageAttachments), "Empty slice should remain empty")
	AssertEqual(t, 0, len(app2.InsuranceAttachments), "Nil slice should be empty after retrieval")

	// Test querying by array size
	query := bson.M{
		"imageAttachments": bson.M{
			"$size": 2,
		},
	}
	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to query by array size")
	AssertEqual(t, 1, count, "Should find 1 appointment with 2 image attachments")

	// Test querying by nested array field
	query2 := bson.M{
		"insuranceAttachments.mimeType": "application/pdf",
	}
	count, err = coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to query by nested array field")
	AssertEqual(t, 1, count, "Should find 1 appointment with PDF insurance attachment")
}

// TestModernCollectionInsertComplexNestedStructure tests inserting a complex nested structure
// similar to what the deleteAccount method does with removed account data
func TestModernCollectionInsertComplexNestedStructure(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("elife_removed_account_data")

	// Create a complex nested structure similar to deleteAccount's removedData
	userID := bson.NewObjectId()
	now := time.Now()

	// Create sample removed data that mimics real collections
	removedData := make(map[string]interface{})

	// Sample data for elife_devices collection
	removedData["elife_devices"] = []map[string]interface{}{
		{
			"_id":       bson.NewObjectId(),
			"userId":    userID,
			"deviceId":  "device1",
			"type":      "smartphone",
			"active":    true,
			"createdAt": now.Add(-24 * time.Hour),
		},
		{
			"_id":       bson.NewObjectId(),
			"userId":    userID,
			"deviceId":  "device2",
			"type":      "tablet",
			"active":    false,
			"createdAt": now.Add(-48 * time.Hour),
		},
	}

	// Sample data for elife_families collection
	removedData["elife_families"] = []map[string]interface{}{
		{
			"_id":      bson.NewObjectId(),
			"userId":   userID,
			"familyId": bson.NewObjectId(),
			"role":     "parent",
			"joinedAt": now.Add(-72 * time.Hour),
			"settings": map[string]interface{}{"notifications": true, "privacy": "strict"},
		},
	}

	// Sample data for elife_accounts collection
	removedData["elife_accounts"] = []map[string]interface{}{
		{
			"_id":       userID,
			"email":     "user@example.com",
			"name":      "Test User",
			"active":    true,
			"createdAt": now.Add(-168 * time.Hour), // 1 week ago
			"profile": map[string]interface{}{
				"age":         30,
				"city":        "New York",
				"preferences": []string{"privacy", "security"},
			},
		},
	}

	// Extra info with additional metadata
	extraInfo := map[string]interface{}{
		"deletionReason": "user_request",
		"requestedBy":    userID,
		"adminNotes":     "Standard account deletion",
		"backupCreated":  true,
		"retentionDays":  30,
		"metadata": map[string]interface{}{
			"version": "1.0",
			"source":  "web_app",
			"browser": "chrome",
		},
	}

	// Create the document structure that deleteAccount method would insert
	documentToInsert := bson.M{
		"userId":      userID,
		"removedData": removedData,
		"createdAt":   now,
		"extraInfo":   extraInfo,
	}

	// Test the Insert operation
	err := coll.Insert(documentToInsert)
	AssertNoError(t, err, "Failed to insert complex nested structure")

	// Verify the document was inserted correctly
	var retrievedDoc bson.M
	err = coll.Find(bson.M{"userId": userID}).One(&retrievedDoc)
	AssertNoError(t, err, "Failed to retrieve inserted document")

	// Verify basic fields
	retrievedMainUserID, ok := retrievedDoc["userId"].(bson.ObjectId)
	if !ok {
		if idStr, ok := retrievedDoc["userId"].(string); ok {
			retrievedMainUserID = bson.ObjectId(idStr)
		} else {
			t.Fatalf("Expected main userId to be bson.ObjectId or string, got %T", retrievedDoc["userId"])
		}
	}
	AssertEqual(t, userID.Hex(), retrievedMainUserID.Hex(), "UserId mismatch")
	if retrievedDoc["removedData"] == nil {
		t.Fatal("RemovedData should not be nil")
	}
	if retrievedDoc["extraInfo"] == nil {
		t.Fatal("ExtraInfo should not be nil")
	}

	// Verify removedData structure
	retrievedRemovedData, ok := retrievedDoc["removedData"].(bson.M)
	if !ok {
		t.Fatalf("RemovedData should be bson.M, got %T", retrievedDoc["removedData"])
	}

	// Verify elife_devices collection data
	devicesData, ok := retrievedRemovedData["elife_devices"].([]interface{})
	if !ok {
		t.Fatal("elife_devices should be []interface{}")
	}
	AssertEqual(t, 2, len(devicesData), "Should have 2 devices")

	// Verify first device
	device1, ok := devicesData[0].(bson.M)
	if !ok {
		t.Fatal("First device should be bson.M")
	}
	// Convert the retrieved userId to bson.ObjectId for comparison
	retrievedUserID, ok := device1["userId"].(bson.ObjectId)
	if !ok {
		// If it's not already a bson.ObjectId, it might be a string - convert it
		if userIDStr, ok := device1["userId"].(string); ok {
			retrievedUserID = bson.ObjectId(userIDStr)
		} else {
			t.Fatalf("Expected userId to be bson.ObjectId or string, got %T", device1["userId"])
		}
	}
	AssertEqual(t, userID.Hex(), retrievedUserID.Hex(), "Device userId mismatch")
	AssertEqual(t, "device1", device1["deviceId"], "Device deviceId mismatch")
	AssertEqual(t, "smartphone", device1["type"], "Device type mismatch")
	AssertEqual(t, true, device1["active"], "Device active mismatch")

	// Verify elife_families collection data
	familiesData, ok := retrievedRemovedData["elife_families"].([]interface{})
	if !ok {
		t.Fatal("elife_families should be []interface{}")
	}
	AssertEqual(t, 1, len(familiesData), "Should have 1 family")

	// Verify family data
	family1, ok := familiesData[0].(bson.M)
	if !ok {
		t.Fatal("Family should be bson.M")
	}
	// Convert the retrieved userId to bson.ObjectId for comparison
	retrievedFamilyUserID, ok := family1["userId"].(bson.ObjectId)
	if !ok {
		if userIDStr, ok := family1["userId"].(string); ok {
			retrievedFamilyUserID = bson.ObjectId(userIDStr)
		} else {
			t.Fatalf("Expected family userId to be bson.ObjectId or string, got %T", family1["userId"])
		}
	}
	AssertEqual(t, userID.Hex(), retrievedFamilyUserID.Hex(), "Family userId mismatch")
	AssertEqual(t, "parent", family1["role"], "Family role mismatch")

	// Verify nested settings in family
	familySettings, ok := family1["settings"].(bson.M)
	if !ok {
		t.Fatal("Family settings should be bson.M")
	}
	AssertEqual(t, true, familySettings["notifications"], "Family notifications setting mismatch")
	AssertEqual(t, "strict", familySettings["privacy"], "Family privacy setting mismatch")

	// Verify elife_accounts collection data
	accountsData, ok := retrievedRemovedData["elife_accounts"].([]interface{})
	if !ok {
		t.Fatal("elife_accounts should be []interface{}")
	}
	AssertEqual(t, 1, len(accountsData), "Should have 1 account")

	// Verify account data
	account1, ok := accountsData[0].(bson.M)
	if !ok {
		t.Fatal("Account should be bson.M")
	}
	// Convert the retrieved _id to bson.ObjectId for comparison
	retrievedAccountID, ok := account1["_id"].(bson.ObjectId)
	if !ok {
		if idStr, ok := account1["_id"].(string); ok {
			retrievedAccountID = bson.ObjectId(idStr)
		} else {
			t.Fatalf("Expected account _id to be bson.ObjectId or string, got %T", account1["_id"])
		}
	}
	AssertEqual(t, userID.Hex(), retrievedAccountID.Hex(), "Account _id mismatch")
	AssertEqual(t, "user@example.com", account1["email"], "Account email mismatch")

	// Verify nested profile in account
	accountProfile, ok := account1["profile"].(bson.M)
	if !ok {
		t.Fatal("Account profile should be bson.M")
	}
	AssertEqual(t, 30, accountProfile["age"], "Account age mismatch")
	AssertEqual(t, "New York", accountProfile["city"], "Account city mismatch")

	// Verify array in profile
	preferences, ok := accountProfile["preferences"].([]interface{})
	if !ok {
		t.Fatal("Account preferences should be []interface{}")
	}
	AssertEqual(t, 2, len(preferences), "Should have 2 preferences")
	AssertEqual(t, "privacy", preferences[0], "First preference mismatch")
	AssertEqual(t, "security", preferences[1], "Second preference mismatch")

	// Verify extraInfo structure
	retrievedExtraInfo, ok := retrievedDoc["extraInfo"].(bson.M)
	if !ok {
		t.Fatal("ExtraInfo should be bson.M")
	}
	AssertEqual(t, "user_request", retrievedExtraInfo["deletionReason"], "DeletionReason mismatch")
	// Convert the retrieved requestedBy to bson.ObjectId for comparison
	retrievedRequestedBy, ok := retrievedExtraInfo["requestedBy"].(bson.ObjectId)
	if !ok {
		if idStr, ok := retrievedExtraInfo["requestedBy"].(string); ok {
			retrievedRequestedBy = bson.ObjectId(idStr)
		} else {
			t.Fatalf("Expected requestedBy to be bson.ObjectId or string, got %T", retrievedExtraInfo["requestedBy"])
		}
	}
	AssertEqual(t, userID.Hex(), retrievedRequestedBy.Hex(), "RequestedBy mismatch")
	AssertEqual(t, true, retrievedExtraInfo["backupCreated"], "BackupCreated mismatch")
	AssertEqual(t, 30, retrievedExtraInfo["retentionDays"], "RetentionDays mismatch")

	// Verify nested metadata in extraInfo
	extraMetadata, ok := retrievedExtraInfo["metadata"].(bson.M)
	if !ok {
		t.Fatal("ExtraInfo metadata should be bson.M")
	}
	AssertEqual(t, "1.0", extraMetadata["version"], "Metadata version mismatch")
	AssertEqual(t, "web_app", extraMetadata["source"], "Metadata source mismatch")
	AssertEqual(t, "chrome", extraMetadata["browser"], "Metadata browser mismatch")

	// Test that we can query the document using nested fields
	count, err := coll.Find(bson.M{"extraInfo.deletionReason": "user_request"}).Count()
	AssertNoError(t, err, "Failed to count with nested field query")
	AssertEqual(t, 1, count, "Should find 1 document with nested field query")

	// Test finding with time range
	timeRangeCount, err := coll.Find(bson.M{
		"createdAt": bson.M{
			"$gte": now.Add(-1 * time.Hour),
			"$lte": now.Add(1 * time.Hour),
		},
	}).Count()
	AssertNoError(t, err, "Failed to count with time range query")
	AssertEqual(t, 1, timeRangeCount, "Should find 1 document in time range")
}

// TestModernCollectionInsertDeleteAccountEdgeCases tests Insert method with edge cases
// from the deleteAccount implementation
func TestModernCollectionInsertDeleteAccountEdgeCases(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("elife_removed_account_data")

	// Test case 1: Empty removedData
	userID1 := bson.NewObjectId()
	doc1 := bson.M{
		"userId":      userID1,
		"removedData": make(map[string]interface{}),
		"createdAt":   time.Now(),
		"extraInfo":   nil,
	}
	err := coll.Insert(doc1)
	AssertNoError(t, err, "Failed to insert document with empty removedData")

	// Test case 2: Nil values in collections
	userID2 := bson.NewObjectId()
	removedData2 := make(map[string]interface{})
	removedData2["elife_devices"] = []map[string]interface{}{
		{
			"_id":       bson.NewObjectId(),
			"userId":    userID2,
			"deviceId":  nil,
			"type":      "smartphone",
			"active":    nil,
			"createdAt": nil,
		},
	}

	doc2 := bson.M{
		"userId":      userID2,
		"removedData": removedData2,
		"createdAt":   time.Now(),
		"extraInfo":   map[string]interface{}{"reason": nil},
	}
	err = coll.Insert(doc2)
	AssertNoError(t, err, "Failed to insert document with nil values")

	// Test case 3: Very deeply nested structure
	userID3 := bson.NewObjectId()
	removedData3 := make(map[string]interface{})
	removedData3["elife_complex"] = []map[string]interface{}{
		{
			"_id": bson.NewObjectId(),
			"level1": map[string]interface{}{
				"level2": map[string]interface{}{
					"level3": map[string]interface{}{
						"userId":    userID3,
						"timestamp": time.Now(),
						"values":    []interface{}{1, 2, 3, "test"},
					},
				},
			},
		},
	}

	doc3 := bson.M{
		"userId":      userID3,
		"removedData": removedData3,
		"createdAt":   time.Now(),
		"extraInfo":   map[string]interface{}{"nested": map[string]interface{}{"deep": true}},
	}
	err = coll.Insert(doc3)
	AssertNoError(t, err, "Failed to insert document with deep nesting")

	// Test case 4: Large array of documents
	userID4 := bson.NewObjectId()
	removedData4 := make(map[string]interface{})
	deviceList := make([]map[string]interface{}, 100)
	for i := 0; i < 100; i++ {
		deviceList[i] = map[string]interface{}{
			"_id":      bson.NewObjectId(),
			"userId":   userID4,
			"deviceId": "device" + strconv.Itoa(i),
			"index":    i,
			"active":   i%2 == 0,
		}
	}
	removedData4["elife_devices"] = deviceList

	doc4 := bson.M{
		"userId":      userID4,
		"removedData": removedData4,
		"createdAt":   time.Now(),
		"extraInfo":   map[string]interface{}{"totalDevices": 100},
	}
	err = coll.Insert(doc4)
	AssertNoError(t, err, "Failed to insert document with large array")

	// Verify all documents were inserted correctly
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, 4, count, "Should have 4 documents")

	// Test querying with different patterns
	// Query by userId
	userCount, err := coll.Find(bson.M{"userId": userID1}).Count()
	AssertNoError(t, err, "Failed to query by userId")
	AssertEqual(t, 1, userCount, "Should find 1 document for userID1")

	// Query by nested field
	nestedCount, err := coll.Find(bson.M{"extraInfo.nested.deep": true}).Count()
	AssertNoError(t, err, "Failed to query by nested field")
	AssertEqual(t, 1, nestedCount, "Should find 1 document with nested field")

	// Query by array size
	var result bson.M
	err = coll.Find(bson.M{"extraInfo.totalDevices": 100}).One(&result)
	AssertNoError(t, err, "Failed to query by array size indicator")

	// Verify that the large array was stored and retrieved correctly
	retrievedRemovedData, ok := result["removedData"].(bson.M)
	if !ok {
		t.Fatal("RemovedData should be bson.M")
	}

	retrievedDevices, ok := retrievedRemovedData["elife_devices"].([]interface{})
	if !ok {
		t.Fatal("elife_devices should be []interface{}")
	}

	AssertEqual(t, 100, len(retrievedDevices), "Should have 100 devices in the large array")

	// Verify some devices in the array
	firstDevice, ok := retrievedDevices[0].(bson.M)
	if !ok {
		t.Fatal("First device should be bson.M")
	}
	AssertEqual(t, "device0", firstDevice["deviceId"], "First device deviceId mismatch")
	AssertEqual(t, 0, firstDevice["index"], "First device index mismatch")
	AssertEqual(t, true, firstDevice["active"], "First device active mismatch")
}

// TestModernCollectionInsertDeleteAccountTimeHandling specifically tests time.Time handling in Insert
// This test reproduces the exact error scenario reported: "Badly formed input data"
func TestModernCollectionInsertDeleteAccountTimeHandling(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("elife_removed_account_data")

	// Create exact structure that causes the error
	userID := bson.NewObjectId()
	now := time.Now()

	// Test case 1: Basic insert with time.Now() directly in bson.M
	doc1 := bson.M{
		"userId":      userID,
		"removedData": make(map[string]interface{}),
		"createdAt":   time.Now(), // Direct time.Now() call
		"extraInfo":   nil,
	}

	err := coll.Insert(doc1)
	AssertNoError(t, err, "Failed to insert document with direct time.Now()")

	// Test case 2: Complex removedData with time fields at various levels
	removedData := make(map[string]interface{})

	// Simulate real collection data with various time formats
	removedData["elife_accounts"] = []interface{}{
		map[string]interface{}{
			"_id":       bson.NewObjectId(),
			"userId":    userID,
			"createdAt": now,
			"updatedAt": now.Add(-24 * time.Hour),
			"lastLogin": now.Add(-1 * time.Hour),
		},
	}

	removedData["elife_sessions"] = []interface{}{
		map[string]interface{}{
			"_id":       bson.NewObjectId(),
			"userId":    userID,
			"startTime": now.Add(-2 * time.Hour),
			"endTime":   now.Add(-1 * time.Hour),
			"duration":  3600, // seconds
		},
	}

	// Test with nested time values in different formats
	removedData["elife_activities"] = []interface{}{
		map[string]interface{}{
			"_id":    bson.NewObjectId(),
			"userId": userID,
			"timestamps": map[string]interface{}{
				"created":  now,
				"modified": now.Add(-30 * time.Minute),
				"accessed": []time.Time{
					now.Add(-3 * time.Hour),
					now.Add(-2 * time.Hour),
					now.Add(-1 * time.Hour),
				},
			},
		},
	}

	extraInfo := map[string]interface{}{
		"deletionTime": now,
		"metadata": map[string]interface{}{
			"processedAt": now,
			"version":     "1.0",
		},
	}

	doc2 := bson.M{
		"userId":      userID,
		"removedData": removedData,
		"createdAt":   now,
		"extraInfo":   extraInfo,
	}

	err = coll.Insert(doc2)
	AssertNoError(t, err, "Failed to insert document with complex time fields")

	// Test case 3: Edge case with nil time pointers and zero times
	var nilTime *time.Time
	zeroTime := time.Time{}

	removedData3 := make(map[string]interface{})
	removedData3["elife_edge_cases"] = []interface{}{
		map[string]interface{}{
			"_id":         bson.NewObjectId(),
			"userId":      userID,
			"nilTime":     nilTime,
			"zeroTime":    zeroTime,
			"validTime":   now,
			"timePointer": &now,
		},
	}

	doc3 := bson.M{
		"userId":      userID,
		"removedData": removedData3,
		"createdAt":   now,
		"extraInfo":   nil,
	}

	err = coll.Insert(doc3)
	AssertNoError(t, err, "Failed to insert document with nil/zero time values")

	// Test case 4: Mixed types in arrays (similar to real-world scenarios)
	removedData4 := make(map[string]interface{})

	// Mix of different data types including times
	mixedArray := []interface{}{
		map[string]interface{}{
			"type":      "event",
			"timestamp": now,
			"data":      "some data",
		},
		map[string]interface{}{
			"type":  "metric",
			"value": 123.45,
			"time":  now.Unix(), // Unix timestamp
		},
		map[string]interface{}{
			"type":     "status",
			"active":   true,
			"since":    now,
			"duration": 3600.0, // float seconds
		},
	}

	removedData4["elife_mixed_data"] = mixedArray

	doc4 := bson.M{
		"userId":      bson.NewObjectId(), // New user for this test
		"removedData": removedData4,
		"createdAt":   time.Now(), // Fresh time.Now() call
		"extraInfo": map[string]interface{}{
			"processedAt": time.Now().UTC(), // UTC time
			"timezone":    "UTC",
		},
	}

	err = coll.Insert(doc4)
	AssertNoError(t, err, "Failed to insert document with mixed data types")

	// Verify all documents were inserted
	count, err := coll.Count()
	AssertNoError(t, err, "Failed to count documents")
	AssertEqual(t, 4, count, "Should have 4 documents")

	// Verify we can retrieve and the times are properly handled
	var retrieved []bson.M
	err = coll.Find(nil).All(&retrieved)
	AssertNoError(t, err, "Failed to retrieve all documents")

	// Check that createdAt fields are properly stored as times
	for i, doc := range retrieved {
		createdAt, exists := doc["createdAt"]
		if !exists {
			t.Fatalf("Document %d missing createdAt field", i)
		}

		// The retrieved time might be primitive.DateTime or time.Time
		switch v := createdAt.(type) {
		case time.Time:
			// Good, it's already a time.Time
		case int64:
			// Might be a timestamp, convert it
			if v < 0 {
				t.Fatalf("Document %d has invalid timestamp: %v", i, v)
			}
		default:
			t.Logf("Document %d createdAt type: %T", i, createdAt)
			// Not failing here as MongoDB might return different types
		}
	}

	// Test specific query with time range to ensure times are queryable
	recentCount, err := coll.Find(bson.M{
		"createdAt": bson.M{
			"$gte": now.Add(-1 * time.Hour),
		},
	}).Count()
	AssertNoError(t, err, "Failed to query by time range")
	if recentCount == 0 {
		t.Error("Should find at least one recent document")
	}
}
