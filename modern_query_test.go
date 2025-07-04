package mgo_test

import (
	"testing"
	"time"

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

// TestModernQueryOrOperator tests $or queries
func TestModernQueryOrOperator(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Insert test data
	userID1 := bson.NewObjectId()
	userID2 := bson.NewObjectId()
	userID3 := bson.NewObjectId()

	appointments := []bson.M{
		{"_id": bson.NewObjectId(), "patientUserId": userID1, "doctorUserId": userID2},
		{"_id": bson.NewObjectId(), "patientUserId": userID2, "doctorUserId": userID1},
		{"_id": bson.NewObjectId(), "patientUserId": userID3, "doctorUserId": userID3},
		{"_id": bson.NewObjectId(), "patientUserId": userID2, "doctorUserId": userID3},
	}

	for _, app := range appointments {
		err := coll.Insert(app)
		AssertNoError(t, err, "Failed to insert appointment")
	}

	// Test $or query - find appointments where user is either patient or doctor
	query := bson.M{
		"$or": []bson.M{
			{"patientUserId": userID1},
			{"doctorUserId": userID1},
		},
	}

	var results []bson.M
	err := coll.Find(query).All(&results)
	AssertNoError(t, err, "Failed to execute $or query")
	AssertEqual(t, 2, len(results), "Should find 2 appointments for userID1")

	// Test more complex $or with additional conditions
	query2 := bson.M{
		"$or": []bson.M{
			{"patientUserId": userID3},
			{"doctorUserId": userID3},
		},
	}
	count, err := coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to count with $or query")
	AssertEqual(t, 2, count, "Should find 2 appointments for userID3")
}

// TestModernQueryTimeRangeFiltering tests time-based queries
func TestModernQueryTimeRangeFiltering(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Insert appointments with different dates
	now := time.Now()
	yesterday := now.Add(-24 * time.Hour)
	twoDaysAgo := now.Add(-48 * time.Hour)
	tomorrow := now.Add(24 * time.Hour)

	appointments := []bson.M{
		{"_id": bson.NewObjectId(), "name": "Past", "startedAt": twoDaysAgo, "createdAt": twoDaysAgo},
		{"_id": bson.NewObjectId(), "name": "Yesterday", "startedAt": yesterday, "createdAt": yesterday},
		{"_id": bson.NewObjectId(), "name": "Today", "startedAt": now, "createdAt": now},
		{"_id": bson.NewObjectId(), "name": "Future", "startedAt": tomorrow, "createdAt": tomorrow},
		{"_id": bson.NewObjectId(), "name": "NotStarted", "startedAt": nil, "createdAt": yesterday},
	}

	for _, app := range appointments {
		err := coll.Insert(app)
		AssertNoError(t, err, "Failed to insert appointment")
	}

	// Test range query with $gte and $lt
	from := yesterday.Add(-1 * time.Hour)
	to := now.Add(1 * time.Hour)
	query := bson.M{
		"startedAt": bson.M{
			"$gte": from,
			"$lt":  to,
		},
	}

	var results []bson.M
	err := coll.Find(query).All(&results)
	AssertNoError(t, err, "Failed to execute time range query")
	AssertEqual(t, 2, len(results), "Should find 2 appointments in range")

	// Test query with only $gte
	query2 := bson.M{
		"startedAt": bson.M{
			"$gte": now,
		},
	}
	count, err := coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to count with $gte query")
	AssertEqual(t, 2, count, "Should find 2 appointments from today onwards")

	// Test query with only $lt
	query3 := bson.M{
		"startedAt": bson.M{
			"$lt": now,
		},
	}
	count, err = coll.Find(query3).Count()
	AssertNoError(t, err, "Failed to count with $lt query")
	AssertEqual(t, 2, count, "Should find 2 appointments before today")

	// Test nil check with $eq
	query4 := bson.M{
		"startedAt": bson.M{
			"$eq": nil,
		},
	}
	count, err = coll.Find(query4).Count()
	AssertNoError(t, err, "Failed to count nil values")
	AssertEqual(t, 1, count, "Should find 1 appointment with nil startedAt")
}

// TestModernQueryNegationOperators tests $ne and $not operators
func TestModernQueryNegationOperators(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Insert test data
	userID1 := bson.NewObjectId()
	userID2 := bson.NewObjectId()
	now := time.Now()

	appointments := []bson.M{
		{"_id": bson.NewObjectId(), "createdBy": userID1, "canceled": true, "startedAt": now},
		{"_id": bson.NewObjectId(), "createdBy": userID1, "canceled": false, "startedAt": nil},
		{"_id": bson.NewObjectId(), "createdBy": userID2, "canceled": true, "startedAt": now},
		{"_id": bson.NewObjectId(), "createdBy": userID2, "startedAt": nil},
		{"_id": bson.NewObjectId(), "createdBy": userID1, "confirmCancel": true},
	}

	for _, app := range appointments {
		err := coll.Insert(app)
		AssertNoError(t, err, "Failed to insert appointment")
	}

	// Test $ne operator
	query := bson.M{
		"createdBy": bson.M{
			"$ne": userID1,
		},
	}
	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to count with $ne query")
	AssertEqual(t, 2, count, "Should find 2 appointments not created by userID1")

	// Test $ne with boolean
	query2 := bson.M{
		"canceled": bson.M{
			"$ne": true,
		},
	}
	count, err = coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to count with $ne boolean query")
	AssertEqual(t, 3, count, "Should find 3 appointments that are not canceled (including missing field)")

	// Test $ne with nil
	query3 := bson.M{
		"startedAt": bson.M{
			"$ne": nil,
		},
	}
	count, err = coll.Find(query3).Count()
	AssertNoError(t, err, "Failed to count non-nil values")
	AssertEqual(t, 2, count, "Should find 2 appointments with non-nil startedAt")
}

// TestModernQueryComplexNotOperator tests complex $not queries
func TestModernQueryComplexNotOperator(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Insert test data with various endedAt values
	now := time.Now()
	past := now.Add(-24 * time.Hour)
	future := now.Add(24 * time.Hour)

	appointments := []bson.M{
		{"_id": bson.NewObjectId(), "name": "Ended Past", "endedAt": past},
		{"_id": bson.NewObjectId(), "name": "Ended Now", "endedAt": now},
		{"_id": bson.NewObjectId(), "name": "Ended Future", "endedAt": future},
		{"_id": bson.NewObjectId(), "name": "Not Ended", "endedAt": nil},
		{"_id": bson.NewObjectId(), "name": "No EndedAt Field"},
	}

	for _, app := range appointments {
		err := coll.Insert(app)
		AssertNoError(t, err, "Failed to insert appointment")
	}

	// Test complex $not query (appointments not done)
	query := bson.M{
		"endedAt": bson.M{
			"$not": bson.M{
				"$ne":  nil,
				"$lte": now,
			},
		},
	}

	var results []bson.M
	err := coll.Find(query).All(&results)
	AssertNoError(t, err, "Failed to execute $not query")
	AssertEqual(t, 3, len(results), "Should find 3 appointments that are not done")

	// Verify the results don't include completed appointments
	for _, result := range results {
		name := result["name"].(string)
		if name == "Ended Past" || name == "Ended Now" {
			t.Fatalf("Query should not return completed appointment: %s", name)
		}
	}
}

// TestModernQueryPaginationWithComplexQuery tests pagination with complex queries
func TestModernQueryPaginationWithComplexQuery(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Insert test data
	now := time.Now()
	for i := 0; i < 25; i++ {
		appointment := bson.M{
			"_id":             bson.NewObjectId(),
			"appointmentType": "consultation",
			"timeForSorting":  now.Add(time.Duration(i) * time.Hour),
			"index":           i,
		}
		if i%2 == 0 {
			appointment["canceled"] = true
		}
		err := coll.Insert(appointment)
		AssertNoError(t, err, "Failed to insert appointment")
	}

	// Test pagination with complex query
	query := bson.M{
		"appointmentType": "consultation",
		"canceled": bson.M{
			"$ne": true,
		},
	}

	// First page
	page := 0
	pageSize := 5
	var results []bson.M
	err := coll.Find(query).Sort("timeForSorting").Skip(pageSize * page).Limit(pageSize).All(&results)
	AssertNoError(t, err, "Failed to get first page")
	AssertEqual(t, 5, len(results), "First page should have 5 results")

	// Verify first page contains correct indices (1, 3, 5, 7, 9)
	for i, result := range results {
		expectedIndex := 1 + (i * 2)
		AssertEqual(t, expectedIndex, result["index"], "Incorrect index in first page")
	}

	// Second page
	page = 1
	results = nil
	err = coll.Find(query).Sort("timeForSorting").Skip(pageSize * page).Limit(pageSize).All(&results)
	AssertNoError(t, err, "Failed to get second page")
	AssertEqual(t, 5, len(results), "Second page should have 5 results")

	// Count total matching documents
	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to count matching documents")
	AssertEqual(t, 12, count, "Should have 12 non-canceled appointments")
}

// TestModernQueryObjectIdHexConversion tests bson.ObjectIdHex usage
func TestModernQueryObjectIdHexConversion(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Create ObjectIds and their hex representations
	id1 := bson.NewObjectId()
	id2 := bson.NewObjectId()
	hex1 := id1.Hex()

	// Insert documents
	appointments := []bson.M{
		{"_id": bson.NewObjectId(), "patientUserId": id1, "doctorUserId": id2},
		{"_id": bson.NewObjectId(), "patientUserId": id2, "doctorUserId": id1},
		{"_id": bson.NewObjectId(), "patientUserId": id1, "doctorUserId": id1},
	}

	for _, app := range appointments {
		err := coll.Insert(app)
		AssertNoError(t, err, "Failed to insert appointment")
	}

	// Query using ObjectIdHex
	query := bson.M{
		"patientUserId": bson.ObjectIdHex(hex1),
	}

	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to count with ObjectIdHex query")
	AssertEqual(t, 2, count, "Should find 2 appointments for patient")

	// Test with $or and ObjectIdHex
	query2 := bson.M{
		"$or": []bson.M{
			{"patientUserId": bson.ObjectIdHex(hex1)},
			{"doctorUserId": bson.ObjectIdHex(hex1)},
		},
	}

	count, err = coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to count with $or and ObjectIdHex")
	AssertEqual(t, 3, count, "Should find all 3 appointments involving user")
}

// TestModernQueryAppointmentListScenario tests a realistic appointment listing scenario
func TestModernQueryAppointmentListScenario(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Create test users
	patientID := bson.NewObjectId()
	doctorID := bson.NewObjectId()
	otherUserID := bson.NewObjectId()

	// Create appointments with various states
	now := time.Now()
	appointments := []bson.M{
		// Active appointment for patient
		{
			"_id":             bson.NewObjectId(),
			"patientUserId":   patientID,
			"doctorUserId":    doctorID,
			"appointmentType": "consultation",
			"startedAt":       now.Add(-1 * time.Hour),
			"createdAt":       now.Add(-2 * time.Hour),
			"createdBy":       patientID,
			"timeForSorting":  now.Add(-1 * time.Hour),
		},
		// Request (not started) appointment
		{
			"_id":             bson.NewObjectId(),
			"patientUserId":   patientID,
			"doctorUserId":    doctorID,
			"appointmentType": "consultation",
			"startedAt":       nil,
			"createdAt":       now.Add(-3 * time.Hour),
			"createdBy":       doctorID,
			"timeForSorting":  now.Add(1 * time.Hour),
		},
		// Canceled appointment
		{
			"_id":             bson.NewObjectId(),
			"patientUserId":   patientID,
			"doctorUserId":    otherUserID,
			"appointmentType": "consultation",
			"startedAt":       now.Add(-24 * time.Hour),
			"createdAt":       now.Add(-25 * time.Hour),
			"createdBy":       patientID,
			"canceled":        true,
			"timeForSorting":  now.Add(-24 * time.Hour),
		},
		// Completed appointment
		{
			"_id":             bson.NewObjectId(),
			"patientUserId":   otherUserID,
			"doctorUserId":    doctorID,
			"appointmentType": "consultation",
			"startedAt":       now.Add(-48 * time.Hour),
			"endedAt":         now.Add(-47 * time.Hour),
			"createdAt":       now.Add(-49 * time.Hour),
			"createdBy":       otherUserID,
			"timeForSorting":  now.Add(-48 * time.Hour),
		},
	}

	for _, app := range appointments {
		err := coll.Insert(app)
		AssertNoError(t, err, "Failed to insert appointment")
	}

	// Test 1: Get all appointments for patient (active, non-canceled)
	query1 := bson.M{
		"patientUserId":   patientID,
		"appointmentType": "consultation",
		"canceled": bson.M{
			"$ne": true,
		},
	}

	var results []bson.M
	err := coll.Find(query1).Sort("timeForSorting").All(&results)
	AssertNoError(t, err, "Failed to find patient appointments")
	AssertEqual(t, 2, len(results), "Should find 2 non-canceled appointments for patient")

	// Test 2: Get request appointments (not started)
	query2 := bson.M{
		"patientUserId":   patientID,
		"appointmentType": "consultation",
		"startedAt": bson.M{
			"$eq": nil,
		},
	}

	count, err := coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to count request appointments")
	AssertEqual(t, 1, count, "Should find 1 request appointment")

	// Test 3: Get appointments created by patient within date range
	from := now.Add(-3 * time.Hour)
	to := now
	query3 := bson.M{
		"patientUserId":   patientID,
		"appointmentType": "consultation",
		"createdBy":       patientID,
		"createdAt": bson.M{
			"$gte": from,
			"$lt":  to,
		},
	}

	count, err = coll.Find(query3).Count()
	AssertNoError(t, err, "Failed to count appointments in date range")
	AssertEqual(t, 1, count, "Should find 1 appointment created by patient in range")

	// Test 4: Complex query for doctor's completed appointments
	query4 := bson.M{
		"doctorUserId":    doctorID,
		"appointmentType": "consultation",
		"endedAt": bson.M{
			"$ne":  nil,
			"$lte": now,
		},
	}

	count, err = coll.Find(query4).Count()
	AssertNoError(t, err, "Failed to count doctor's completed appointments")
	AssertEqual(t, 1, count, "Should find 1 completed appointment for doctor")
}

// TestModernQueryAppointmentWithTimeCandidates tests saving and retrieving Appointment struct with time array field
func TestModernQueryAppointmentWithTimeCandidates(t *testing.T) {
	// Define the Appointment struct with StartedAtCandidates
	type Appointment struct {
		ID                  bson.ObjectId `json:"id" bson:"_id,omitempty"`
		StartedAtCandidates []time.Time   `json:"startedAtCandidates" bson:"startedAtCandidates"`
		CreatedAt           time.Time     `json:"createdAt" bson:"createdAt"`
		UpdatedAt           time.Time     `json:"updatedAt" bson:"updatedAt"`
	}

	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Create test appointments with various time candidates
	now := time.Now()
	tomorrow := now.Add(24 * time.Hour)
	nextWeek := now.Add(7 * 24 * time.Hour)

	appointments := []Appointment{
		// Appointment with multiple time candidates
		{
			ID: bson.NewObjectId(),
			StartedAtCandidates: []time.Time{
				now,
				tomorrow,
				nextWeek,
			},
			CreatedAt: now.Add(-1 * time.Hour),
			UpdatedAt: now,
		},
		// Appointment with single time candidate
		{
			ID:                  bson.NewObjectId(),
			StartedAtCandidates: []time.Time{tomorrow},
			CreatedAt:           now.Add(-2 * time.Hour),
			UpdatedAt:           now.Add(-30 * time.Minute),
		},
		// Appointment with empty time candidates
		{
			ID:                  bson.NewObjectId(),
			StartedAtCandidates: []time.Time{},
			CreatedAt:           now.Add(-3 * time.Hour),
			UpdatedAt:           now.Add(-1 * time.Hour),
		},
		// Appointment with nil time candidates
		{
			ID:                  bson.NewObjectId(),
			StartedAtCandidates: nil,
			CreatedAt:           now.Add(-4 * time.Hour),
			UpdatedAt:           now.Add(-2 * time.Hour),
		},
	}

	// Insert all appointments
	insertedIDs := make([]bson.ObjectId, 0, len(appointments))
	for _, app := range appointments {
		err := coll.Insert(app)
		AssertNoError(t, err, "Failed to insert appointment")
		insertedIDs = append(insertedIDs, app.ID)
	}

	// Test 1: Retrieve all appointments and convert back to struct
	var results []Appointment
	err := coll.Find(nil).All(&results)
	AssertNoError(t, err, "Failed to retrieve all appointments")
	AssertEqual(t, len(appointments), len(results), "Should retrieve all inserted appointments")

	// Verify each appointment was saved and retrieved correctly
	resultMap := make(map[string]Appointment)
	for _, result := range results {
		resultMap[result.ID.Hex()] = result
	}

	for _, original := range appointments {
		retrieved, exists := resultMap[original.ID.Hex()]
		if !exists {
			t.Fatalf("Appointment with ID %s not found in results", original.ID.Hex())
		}

		// Verify ID
		AssertEqual(t, original.ID, retrieved.ID, "ID mismatch")

		// Verify StartedAtCandidates
		if original.StartedAtCandidates == nil {
			if retrieved.StartedAtCandidates != nil && len(retrieved.StartedAtCandidates) > 0 {
				t.Fatalf("Expected nil StartedAtCandidates, got %v", retrieved.StartedAtCandidates)
			}
		} else {
			AssertEqual(t, len(original.StartedAtCandidates), len(retrieved.StartedAtCandidates),
				"StartedAtCandidates length mismatch")

			for i, originalTime := range original.StartedAtCandidates {
				// Compare times with millisecond precision (MongoDB stores with millisecond precision)
				originalTrunc := originalTime.Truncate(time.Millisecond)
				retrievedTrunc := retrieved.StartedAtCandidates[i].Truncate(time.Millisecond)
				if !originalTrunc.Equal(retrievedTrunc) {
					t.Fatalf("StartedAtCandidates[%d] mismatch: expected %v, got %v",
						i, originalTrunc, retrievedTrunc)
				}
			}
		}

		// Verify timestamps (with millisecond precision)
		originalCreatedAt := original.CreatedAt.Truncate(time.Millisecond)
		retrievedCreatedAt := retrieved.CreatedAt.Truncate(time.Millisecond)
		if !originalCreatedAt.Equal(retrievedCreatedAt) {
			t.Fatalf("CreatedAt mismatch: expected %v, got %v", originalCreatedAt, retrievedCreatedAt)
		}

		originalUpdatedAt := original.UpdatedAt.Truncate(time.Millisecond)
		retrievedUpdatedAt := retrieved.UpdatedAt.Truncate(time.Millisecond)
		if !originalUpdatedAt.Equal(retrievedUpdatedAt) {
			t.Fatalf("UpdatedAt mismatch: expected %v, got %v", originalUpdatedAt, retrievedUpdatedAt)
		}
	}

	// Test 2: Query appointments with non-empty StartedAtCandidates
	var nonEmptyResults []Appointment
	query := bson.M{
		"startedAtCandidates": bson.M{
			"$exists": true,
			"$ne":     []time.Time{},
		},
	}
	err = coll.Find(query).All(&nonEmptyResults)
	AssertNoError(t, err, "Failed to query non-empty StartedAtCandidates")
	AssertEqual(t, 2, len(nonEmptyResults), "Should find 2 appointments with non-empty StartedAtCandidates")

	// Test 3: Query appointments created within a time range
	from := now.Add(-3 * time.Hour)
	to := now.Add(-1 * time.Hour)
	var rangeResults []Appointment
	rangeQuery := bson.M{
		"createdAt": bson.M{
			"$gte": from,
			"$lt":  to,
		},
	}
	err = coll.Find(rangeQuery).Sort("-createdAt").All(&rangeResults)
	AssertNoError(t, err, "Failed to query appointments in time range")
	AssertEqual(t, 2, len(rangeResults), "Should find 2 appointments in the time range")

	// Verify sorting order (newest first)
	if len(rangeResults) > 1 {
		for i := 1; i < len(rangeResults); i++ {
			prevTime := rangeResults[i-1].CreatedAt
			currTime := rangeResults[i].CreatedAt
			if prevTime.Before(currTime) {
				t.Fatal("Results not sorted correctly by createdAt (descending)")
			}
		}
	}

	// Test 4: Find appointment by specific ID using Find().One()
	targetID := insertedIDs[0]
	var singleResult Appointment
	err = coll.Find(bson.M{"_id": targetID}).One(&singleResult)
	AssertNoError(t, err, "Failed to find appointment by ID")
	AssertEqual(t, targetID, singleResult.ID, "Retrieved wrong appointment")
	AssertEqual(t, 3, len(singleResult.StartedAtCandidates), "Should have 3 time candidates")
}

// TestModernQueryOneWithTimeArray tests that Find().One() properly decodes arrays of time.Time values
func TestModernQueryOneWithTimeArray(t *testing.T) {
	// Define the Appointment struct
	type Appointment struct {
		ID                  bson.ObjectId `json:"id" bson:"_id,omitempty"`
		StartedAtCandidates []time.Time   `json:"startedAtCandidates" bson:"startedAtCandidates"`
		CreatedAt           time.Time     `json:"createdAt" bson:"createdAt"`
		UpdatedAt           time.Time     `json:"updatedAt" bson:"updatedAt"`
	}

	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Create a test appointment
	now := time.Now()
	appointment := Appointment{
		ID: bson.NewObjectId(),
		StartedAtCandidates: []time.Time{
			now,
			now.Add(24 * time.Hour),
			now.Add(48 * time.Hour),
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	// Insert the appointment
	err := coll.Insert(appointment)
	AssertNoError(t, err, "Failed to insert appointment")

	// Test 1: Retrieve with Find().All() - this works
	var allResults []Appointment
	err = coll.Find(bson.M{"_id": appointment.ID}).All(&allResults)
	AssertNoError(t, err, "Failed to retrieve with All()")
	AssertEqual(t, 1, len(allResults), "Should find exactly one appointment")
	AssertEqual(t, 3, len(allResults[0].StartedAtCandidates), "All() should retrieve 3 time candidates")

	// Test 2: Retrieve with Find().One() - now properly handles time arrays
	var oneResult Appointment
	err = coll.Find(bson.M{"_id": appointment.ID}).One(&oneResult)
	AssertNoError(t, err, "Failed to retrieve with One()")

	// Verify One() properly decodes time arrays
	AssertEqual(t, 3, len(oneResult.StartedAtCandidates), "One() should retrieve 3 time candidates")

	// Verify the times are correct
	for i, expectedTime := range appointment.StartedAtCandidates {
		actualTime := oneResult.StartedAtCandidates[i]
		expectedTrunc := expectedTime.Truncate(time.Millisecond)
		actualTrunc := actualTime.Truncate(time.Millisecond)
		if !expectedTrunc.Equal(actualTrunc) {
			t.Errorf("Time mismatch at index %d: expected %v, got %v", i, expectedTrunc, actualTrunc)
		}
	}

	// Test 3: Verify both All() and One() produce the same results
	AssertEqual(t, len(allResults[0].StartedAtCandidates), len(oneResult.StartedAtCandidates),
		"All() and One() should return the same number of time candidates")
}
