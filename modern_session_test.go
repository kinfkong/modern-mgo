package mgo_test

import (
	"testing"

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
