package mgo_test

import (
	"testing"

	"github.com/globalsign/mgo/bson"
)

// TestBsonObjectIdHexConversion tests ObjectIdHex conversion functionality
func TestBsonObjectIdHexConversion(t *testing.T) {
	// Test creating ObjectId from valid hex string
	hexString := "507f1f77bcf86cd799439011"
	oid := bson.ObjectIdHex(hexString)

	// Verify conversion back to hex
	AssertEqual(t, hexString, oid.Hex(), "ObjectId hex conversion mismatch")

	// Test that ObjectIdHex panics with invalid hex string
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic for invalid hex string")
		}
	}()

	// This should panic
	_ = bson.ObjectIdHex("invalid-hex")
}

// TestBsonObjectIdIsObjectIdHex tests IsObjectIdHex validation
func TestBsonObjectIdIsObjectIdHex(t *testing.T) {
	// Valid hex strings
	validHexStrings := []string{
		"507f1f77bcf86cd799439011",
		"507f191e810c19729de860ea",
		bson.NewObjectId().Hex(),
	}

	for _, hex := range validHexStrings {
		if !bson.IsObjectIdHex(hex) {
			t.Errorf("Expected %s to be valid ObjectId hex", hex)
		}
	}

	// Invalid hex strings
	invalidHexStrings := []string{
		"",
		"507f1f77bcf86cd79943901",   // Too short
		"507f1f77bcf86cd7994390111", // Too long
		"507f1f77bcf86cd79943901g",  // Invalid character
		"not-a-hex-string",
		"GGGGGGGGGGGGGGGGGGGGGGGG",
	}

	for _, hex := range invalidHexStrings {
		if bson.IsObjectIdHex(hex) {
			t.Errorf("Expected %s to be invalid ObjectId hex", hex)
		}
	}
}

// TestBsonObjectIdInQueries tests using ObjectIds in queries
func TestBsonObjectIdInQueries(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_objectids")

	// Create documents with ObjectId references
	userID1 := bson.NewObjectId()
	userID2 := bson.NewObjectId()
	userID1Hex := userID1.Hex()
	userID2Hex := userID2.Hex()

	docs := []bson.M{
		{
			"_id":     bson.NewObjectId(),
			"userId":  userID1,
			"groupId": userID2,
			"type":    "A",
		},
		{
			"_id":     bson.NewObjectId(),
			"userId":  userID2,
			"groupId": userID1,
			"type":    "B",
		},
		{
			"_id":     bson.NewObjectId(),
			"userId":  userID1,
			"groupId": userID1,
			"type":    "C",
		},
	}

	for _, doc := range docs {
		err := coll.Insert(doc)
		AssertNoError(t, err, "Failed to insert document")
	}

	// Test 1: Query using ObjectId directly
	count, err := coll.Find(bson.M{"userId": userID1}).Count()
	AssertNoError(t, err, "Failed to count with ObjectId query")
	AssertEqual(t, 2, count, "Should find 2 documents with userID1")

	// Test 2: Query using ObjectIdHex conversion
	count, err = coll.Find(bson.M{"userId": bson.ObjectIdHex(userID1Hex)}).Count()
	AssertNoError(t, err, "Failed to count with ObjectIdHex query")
	AssertEqual(t, 2, count, "Should find 2 documents with ObjectIdHex")

	// Test 3: Query with multiple ObjectId conditions
	query := bson.M{
		"userId":  bson.ObjectIdHex(userID1Hex),
		"groupId": bson.ObjectIdHex(userID2Hex),
	}
	var result bson.M
	err = coll.Find(query).One(&result)
	AssertNoError(t, err, "Failed to find with multiple ObjectId conditions")
	AssertEqual(t, "A", result["type"], "Found wrong document")

	// Test 4: $in query with ObjectIds
	query2 := bson.M{
		"userId": bson.M{
			"$in": []bson.ObjectId{userID1, userID2},
		},
	}
	count, err = coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to count with $in ObjectId query")
	AssertEqual(t, 3, count, "Should find all 3 documents")

	// Test 5: Mixed ObjectId and ObjectIdHex in $or
	query3 := bson.M{
		"$or": []bson.M{
			{"userId": userID1},                       // Direct ObjectId
			{"groupId": bson.ObjectIdHex(userID1Hex)}, // ObjectIdHex
		},
	}
	count, err = coll.Find(query3).Count()
	AssertNoError(t, err, "Failed to count with mixed ObjectId/$or query")
	AssertEqual(t, 3, count, "Should find 3 documents with mixed query")
}

// TestBsonObjectIdArrayOperations tests ObjectId arrays in documents
func TestBsonObjectIdArrayOperations(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_objectid_arrays")

	// Create ObjectIds
	id1 := bson.NewObjectId()
	id2 := bson.NewObjectId()
	id3 := bson.NewObjectId()

	// Insert document with ObjectId array
	doc := bson.M{
		"_id":       bson.NewObjectId(),
		"name":      "Test Group",
		"memberIds": []bson.ObjectId{id1, id2, id3},
		"adminIds":  []bson.ObjectId{id1},
	}

	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert document with ObjectId arrays")

	// Test 1: Retrieve and verify ObjectId arrays
	var result bson.M
	err = coll.FindId(doc["_id"]).One(&result)
	AssertNoError(t, err, "Failed to retrieve document")

	memberIds, ok := result["memberIds"].([]bson.ObjectId)
	if !ok {
		// Try interface slice (MongoDB driver may return this)
		memberIdsInterface, ok := result["memberIds"].([]interface{})
		if !ok {
			t.Fatal("memberIds is not a slice")
		}
		memberIds = make([]bson.ObjectId, len(memberIdsInterface))
		for i, v := range memberIdsInterface {
			// Handle different possible types returned by MongoDB
			switch id := v.(type) {
			case bson.ObjectId:
				memberIds[i] = id
			case []byte:
				if len(id) == 12 {
					memberIds[i] = bson.ObjectId(id)
				} else {
					t.Fatalf("Invalid ObjectId byte length: %d", len(id))
				}
			default:
				t.Fatalf("Unexpected type for ObjectId: %T", v)
			}
		}
	}

	AssertEqual(t, 3, len(memberIds), "Should have 3 member IDs")
	AssertEqual(t, id1, memberIds[0], "First member ID mismatch")

	// Test 2: Query using $in with ObjectId array field
	query := bson.M{
		"memberIds": bson.M{
			"$in": []bson.ObjectId{id2},
		},
	}
	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to query with $in on ObjectId array")
	AssertEqual(t, 1, count, "Should find 1 document containing id2")

	// Test 3: Query using $all with multiple ObjectIds
	query2 := bson.M{
		"memberIds": bson.M{
			"$all": []bson.ObjectId{id1, id3},
		},
	}
	count, err = coll.Find(query2).Count()
	AssertNoError(t, err, "Failed to query with $all on ObjectId array")
	AssertEqual(t, 1, count, "Should find 1 document containing both id1 and id3")

	// Test 4: Update ObjectId array
	err = coll.UpdateId(doc["_id"], bson.M{
		"$push": bson.M{
			"adminIds": id2,
		},
	})
	AssertNoError(t, err, "Failed to push to ObjectId array")

	// Verify update
	err = coll.FindId(doc["_id"]).One(&result)
	AssertNoError(t, err, "Failed to retrieve updated document")

	adminIdsInterface, ok := result["adminIds"].([]interface{})
	if !ok {
		// Try direct ObjectId slice
		adminIdsDirect, ok := result["adminIds"].([]bson.ObjectId)
		if ok {
			AssertEqual(t, 2, len(adminIdsDirect), "Should have 2 admin IDs after push")
			return
		}
		t.Fatal("adminIds is not a slice after update")
	}
	AssertEqual(t, 2, len(adminIdsInterface), "Should have 2 admin IDs after push")
}

// TestBsonObjectIdNilHandling tests nil ObjectId handling
func TestBsonObjectIdNilHandling(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("test_nil_objectids")

	// Test document with optional ObjectId field
	type DocumentWithOptionalRef struct {
		ID       bson.ObjectId  `bson:"_id,omitempty"`
		Name     string         `bson:"name"`
		ParentID *bson.ObjectId `bson:"parentId,omitempty"`
	}

	// Insert document with nil ParentID
	doc1 := DocumentWithOptionalRef{
		ID:       bson.NewObjectId(),
		Name:     "Root",
		ParentID: nil,
	}

	err := coll.Insert(doc1)
	AssertNoError(t, err, "Failed to insert document with nil ObjectId pointer")

	// Insert document with valid ParentID
	parentID := bson.NewObjectId()
	doc2 := DocumentWithOptionalRef{
		ID:       bson.NewObjectId(),
		Name:     "Child",
		ParentID: &parentID,
	}

	err = coll.Insert(doc2)
	AssertNoError(t, err, "Failed to insert document with ObjectId pointer")

	// Query for documents with no parent
	var roots []DocumentWithOptionalRef
	err = coll.Find(bson.M{"parentId": nil}).All(&roots)
	AssertNoError(t, err, "Failed to find documents with nil parentId")
	AssertEqual(t, 1, len(roots), "Should find 1 root document")
	AssertEqual(t, "Root", roots[0].Name, "Wrong root document")

	// Query for documents with parent
	var children []DocumentWithOptionalRef
	err = coll.Find(bson.M{"parentId": bson.M{"$ne": nil}}).All(&children)
	AssertNoError(t, err, "Failed to find documents with non-nil parentId")
	AssertEqual(t, 1, len(children), "Should find 1 child document")
	AssertEqual(t, "Child", children[0].Name, "Wrong child document")
}

// TestBsonObjectIdComplexScenarios tests complex real-world ObjectId scenarios
func TestBsonObjectIdComplexScenarios(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	coll := tdb.C("appointments")

	// Simulate the appointment scenario from the example
	patientUserID := bson.NewObjectId()
	doctorUserID := bson.NewObjectId()
	createdByID := bson.NewObjectId()

	// Convert to hex strings (simulating receiving from API)
	patientHex := patientUserID.Hex()

	// Insert appointments
	appointments := []bson.M{
		{
			"_id":             bson.NewObjectId(),
			"patientUserId":   patientUserID,
			"doctorUserId":    doctorUserID,
			"appointmentType": "consultation",
			"createdBy":       createdByID,
		},
		{
			"_id":             bson.NewObjectId(),
			"patientUserId":   doctorUserID,  // Doctor as patient
			"doctorUserId":    patientUserID, // Patient as doctor (role swap)
			"appointmentType": "consultation",
			"createdBy":       doctorUserID,
		},
	}

	for _, app := range appointments {
		err := coll.Insert(app)
		AssertNoError(t, err, "Failed to insert appointment")
	}

	// Complex query simulating ListAppointments logic
	// Find appointments where user is either patient or doctor
	userID := patientHex // Simulating receiving from API
	query := bson.M{
		"$or": []bson.M{
			{"patientUserId": bson.ObjectIdHex(userID)},
			{"doctorUserId": bson.ObjectIdHex(userID)},
		},
		"appointmentType": "consultation",
	}

	if bson.IsObjectIdHex(userID) {
		// Additional condition based on creator
		query["createdBy"] = bson.M{
			"$ne": bson.ObjectIdHex(userID),
		}
	}

	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to execute complex query")
	AssertEqual(t, 2, count, "Should find 2 appointments where user is involved but didn't create")

	// Test with invalid hex string handling
	invalidUserID := "invalid-user-id"
	if bson.IsObjectIdHex(invalidUserID) {
		t.Fatal("Invalid hex should not pass IsObjectIdHex")
	}
}
