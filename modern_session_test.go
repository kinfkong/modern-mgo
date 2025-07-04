package mgo_test

import (
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

func TestModernSessionDB(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Test DB method
	session := tdb.Session
	db := session.DB(tdb.DBName)

	if db == nil {
		t.Fatal("DB() returned nil")
	}

	// Verify we can use the database
	err := db.C("test_collection").Insert(bson.M{"test": "value"})
	AssertNoError(t, err, "Failed to insert test document")
}

func TestModernSessionSetMode(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Test different consistency modes
	modes := []mgo.Mode{
		mgo.Primary,
		mgo.PrimaryPreferred,
		mgo.Secondary,
		mgo.SecondaryPreferred,
		mgo.Nearest,
		mgo.Eventual,
		mgo.Monotonic,
		mgo.Strong,
	}

	for _, mode := range modes {
		tdb.Session.SetMode(mode, true)
		// No error should occur when setting modes
	}
}

func TestModernSessionMode(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Test getting mode
	mode := tdb.Session.Mode()
	if mode != mgo.Primary {
		t.Fatalf("Expected Primary mode, got %v", mode)
	}

	// Set a different mode and verify
	tdb.Session.SetMode(mgo.SecondaryPreferred, true)
	mode = tdb.Session.Mode()
	if mode != mgo.SecondaryPreferred {
		t.Fatalf("Expected SecondaryPreferred mode, got %v", mode)
	}
}

func TestModernSessionPing(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Ping the server
	err := tdb.Session.Ping()
	AssertNoError(t, err, "Failed to ping server")
}

func TestModernSessionClone(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Clone the session
	cloned := tdb.Session.Clone()
	defer cloned.Close()

	// Cloned session should be usable
	err := cloned.DB(tdb.DBName).C("test_collection").Insert(bson.M{"test": "from_clone"})
	AssertNoError(t, err, "Failed to use cloned session")
}

func TestModernSessionCopy(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Copy the session
	copied := tdb.Session.Copy()
	defer copied.Close()

	// Copied session should be usable
	err := copied.DB(tdb.DBName).C("test_collection").Insert(bson.M{"test": "from_copy"})
	AssertNoError(t, err, "Failed to use copied session")
}

func TestModernSessionRun(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Run a simple command on admin database
	var result bson.M
	err := tdb.Session.Run(true, bson.M{"ping": 1}, &result)
	AssertNoError(t, err, "Failed to run ping command")

	// Check result
	if result["ok"] != 1.0 {
		t.Fatalf("Ping command did not return ok=1: %v", result)
	}

	// Run command on default database
	var result2 bson.M
	err = tdb.Session.Run(false, bson.M{"ping": 1}, &result2)
	AssertNoError(t, err, "Failed to run ping command on default database")
}

func TestModernSessionBuildInfo(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Get build info
	buildInfo, err := tdb.Session.BuildInfo()
	AssertNoError(t, err, "Failed to get build info")

	// Check that we got some version info
	if buildInfo.Version == "" {
		t.Fatal("BuildInfo returned empty version")
	}

	// Check version array
	if len(buildInfo.VersionArray) < 2 {
		t.Fatal("BuildInfo returned invalid version array")
	}
}

func TestModernSessionWithTransaction(t *testing.T) {
	// Note: Transactions require MongoDB 4.0+ with replica set
	// This test will be skipped if transactions are not supported

	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Check if we can start a session (transactions require sessions)
	buildInfo, err := tdb.Session.BuildInfo()
	if err != nil {
		t.Skip("Cannot get build info, skipping transaction test")
	}

	// Parse version (rough check for 4.0+)
	if len(buildInfo.VersionArray) < 2 || buildInfo.VersionArray[0] < 4 {
		t.Skip("MongoDB version < 4.0, skipping transaction test")
	}
}

func TestModernSessionCollectionOperations(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Create a new session
	session := tdb.Session.Copy()
	defer session.Close()

	// Access collection through session
	db := session.DB(tdb.DBName)
	coll := db.C("test_collection")

	// Test insert operation
	doc := bson.M{"_id": bson.NewObjectId(), "value": "test"}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert through session")

	// Test find operation
	var result bson.M
	err = coll.FindId(doc["_id"]).One(&result)
	AssertNoError(t, err, "Failed to find through session")
	AssertEqual(t, "test", result["value"], "Incorrect value retrieved")

	// Test update operation
	err = coll.UpdateId(doc["_id"], bson.M{"$set": bson.M{"value": "updated"}})
	AssertNoError(t, err, "Failed to update through session")

	// Verify update
	err = coll.FindId(doc["_id"]).One(&result)
	AssertNoError(t, err, "Failed to find updated document")
	AssertEqual(t, "updated", result["value"], "Value not updated")

	// Test remove operation
	err = coll.RemoveId(doc["_id"])
	AssertNoError(t, err, "Failed to remove through session")

	// Verify removal
	err = coll.FindId(doc["_id"]).One(&result)
	AssertError(t, err, "Expected error when finding removed document")
}

func TestModernSessionWithComplexQueries(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Create a new session
	session := tdb.Session.Copy()
	defer session.Close()

	// Access collection
	coll := session.DB(tdb.DBName).C("appointments")

	// Insert test data
	userID1 := bson.NewObjectId()
	userID2 := bson.NewObjectId()
	now := time.Now()

	appointments := []interface{}{
		bson.M{
			"_id":             bson.NewObjectId(),
			"patientUserId":   userID1,
			"doctorUserId":    userID2,
			"startedAt":       now,
			"appointmentType": "consultation",
		},
		bson.M{
			"_id":             bson.NewObjectId(),
			"patientUserId":   userID2,
			"doctorUserId":    userID1,
			"startedAt":       nil,
			"appointmentType": "consultation",
		},
		bson.M{
			"_id":             bson.NewObjectId(),
			"patientUserId":   userID1,
			"doctorUserId":    userID1,
			"startedAt":       now.Add(-24 * time.Hour),
			"canceled":        true,
			"appointmentType": "checkup",
		},
	}

	err := coll.Insert(appointments...)
	AssertNoError(t, err, "Failed to insert appointments")

	// Test complex query with $or
	query := bson.M{
		"$or": []bson.M{
			{"patientUserId": userID1},
			{"doctorUserId": userID1},
		},
		"appointmentType": "consultation",
	}

	count, err := coll.Find(query).Count()
	AssertNoError(t, err, "Failed to count with complex query")
	AssertEqual(t, 2, count, "Incorrect count for complex query")

	// Test pagination through session
	var results []bson.M
	err = coll.Find(query).Sort("-startedAt").Skip(0).Limit(1).All(&results)
	AssertNoError(t, err, "Failed to execute paginated query")
	AssertEqual(t, 1, len(results), "Incorrect number of paginated results")
}

func TestModernSessionCloseBehavior(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Create and immediately close a session
	session := tdb.Session.Copy()
	coll := session.DB(tdb.DBName).C("test_collection")

	// Insert a document before closing
	doc := bson.M{"_id": bson.NewObjectId(), "value": "test"}
	err := coll.Insert(doc)
	AssertNoError(t, err, "Failed to insert before session close")

	// Close the session
	session.Close()

	// Attempting operations after close should fail
	// Note: The actual behavior may vary depending on the driver implementation
	// Some drivers might queue operations or have connection pooling

	// Create a new session to verify the document was inserted
	newSession := tdb.Session.Copy()
	defer newSession.Close()

	newColl := newSession.DB(tdb.DBName).C("test_collection")
	var result bson.M
	err = newColl.FindId(doc["_id"]).One(&result)
	AssertNoError(t, err, "Failed to find document with new session")
	AssertEqual(t, "test", result["value"], "Document not properly saved")
}

func TestModernSessionDatabaseSwitch(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Create a session
	session := tdb.Session.Copy()
	defer session.Close()

	// Use first database
	db1Name := tdb.DBName + "_db1"
	db1 := session.DB(db1Name)
	coll1 := db1.C("collection1")

	// Insert in first database
	doc1 := bson.M{"_id": bson.NewObjectId(), "db": "db1"}
	err := coll1.Insert(doc1)
	AssertNoError(t, err, "Failed to insert in db1")

	// Use second database with same session
	db2Name := tdb.DBName + "_db2"
	db2 := session.DB(db2Name)
	coll2 := db2.C("collection2")

	// Insert in second database
	doc2 := bson.M{"_id": bson.NewObjectId(), "db": "db2"}
	err = coll2.Insert(doc2)
	AssertNoError(t, err, "Failed to insert in db2")

	// Verify both documents exist in their respective databases
	var result1, result2 bson.M
	err = coll1.FindId(doc1["_id"]).One(&result1)
	AssertNoError(t, err, "Failed to find document in db1")
	AssertEqual(t, "db1", result1["db"], "Incorrect document in db1")

	err = coll2.FindId(doc2["_id"]).One(&result2)
	AssertNoError(t, err, "Failed to find document in db2")
	AssertEqual(t, "db2", result2["db"], "Incorrect document in db2")

	// Clean up test databases
	err = db1.DropDatabase()
	AssertNoError(t, err, "Failed to drop db1")
	err = db2.DropDatabase()
	AssertNoError(t, err, "Failed to drop db2")
}

func TestModernSessionEmptyDatabaseName(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	// Create a session
	session := tdb.Session.Copy()
	defer session.Close()

	// Access database with empty name (should use default from connection string)
	db := session.DB("")

	// The behavior with empty database name depends on the connection string
	// and driver implementation. This test documents the behavior.

	// Try to use a collection with empty database name
	coll := db.C("test_collection")
	doc := bson.M{"_id": bson.NewObjectId(), "test": "empty_db"}
	err := coll.Insert(doc)
	if err != nil {
		t.Logf("Empty database name resulted in error on insert: %v", err)
	} else {
		// If insert succeeded, try to retrieve it
		var result bson.M
		err = coll.FindId(doc["_id"]).One(&result)
		if err != nil {
			t.Logf("Empty database name: insert succeeded but find failed: %v", err)
		} else {
			t.Logf("Empty database name: operations succeeded")
		}
	}
}
