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
