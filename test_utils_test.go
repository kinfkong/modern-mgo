package mgo_test

import (
	"os"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// TestDB holds the test database connection and name
type TestDB struct {
	Session *mgo.Session
	DBName  string
}

// NewTestDB creates a new test database connection
func NewTestDB(t *testing.T) *TestDB {
	// Get MongoDB URL from environment or use default
	mongoURL := os.Getenv("MONGODB_TEST_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27018/modern_mgo_test"
	}

	// Connect to MongoDB
	session, err := mgo.DialWithTimeout(mongoURL, 30*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to test MongoDB: %v", err)
	}

	// Create a unique database name for this test run
	dbName := "modern_mgo_test_" + bson.NewObjectId().Hex()

	return &TestDB{
		Session: session,
		DBName:  dbName,
	}
}

// Close closes the test database connection and drops the test database
func (tdb *TestDB) Close(t *testing.T) {
	if tdb.Session != nil {
		// Drop the test database
		err := tdb.Session.DB(tdb.DBName).DropDatabase()
		if err != nil {
			t.Logf("Warning: Failed to drop test database: %v", err)
		}
		tdb.Session.Close()
	}
}

// C returns a collection from the test database
func (tdb *TestDB) C(collection string) *mgo.Collection {
	return tdb.Session.DB(tdb.DBName).C(collection)
}

// DB returns the test database
func (tdb *TestDB) DB() *mgo.ModernDB {
	return tdb.Session.DB(tdb.DBName)
}

// TestData provides sample data for testing
type TestData struct {
	// Sample documents
	Users    []bson.M
	Products []bson.M
	Orders   []bson.M
}

// GetTestData returns sample test data
func GetTestData() *TestData {
	return &TestData{
		Users: []bson.M{
			{
				"_id":       bson.NewObjectId(),
				"name":      "John Doe",
				"email":     "john@example.com",
				"age":       30,
				"active":    true,
				"createdAt": time.Now(),
			},
			{
				"_id":       bson.NewObjectId(),
				"name":      "Jane Smith",
				"email":     "jane@example.com",
				"age":       25,
				"active":    true,
				"createdAt": time.Now().Add(-24 * time.Hour),
			},
			{
				"_id":       bson.NewObjectId(),
				"name":      "Bob Johnson",
				"email":     "bob@example.com",
				"age":       35,
				"active":    false,
				"createdAt": time.Now().Add(-48 * time.Hour),
			},
		},
		Products: []bson.M{
			{
				"_id":       bson.NewObjectId(),
				"name":      "Product A",
				"price":     100.50,
				"category":  "Electronics",
				"inStock":   true,
				"quantity":  50,
				"tags":      []string{"new", "featured"},
				"createdAt": time.Now(),
			},
			{
				"_id":       bson.NewObjectId(),
				"name":      "Product B",
				"price":     50.25,
				"category":  "Books",
				"inStock":   true,
				"quantity":  100,
				"tags":      []string{"bestseller"},
				"createdAt": time.Now().Add(-24 * time.Hour),
			},
			{
				"_id":       bson.NewObjectId(),
				"name":      "Product C",
				"price":     200.00,
				"category":  "Electronics",
				"inStock":   false,
				"quantity":  0,
				"tags":      []string{"premium", "out-of-stock"},
				"createdAt": time.Now().Add(-48 * time.Hour),
			},
		},
		Orders: []bson.M{
			{
				"_id":       bson.NewObjectId(),
				"userId":    bson.NewObjectId(),
				"products":  []bson.ObjectId{bson.NewObjectId(), bson.NewObjectId()},
				"total":     150.75,
				"status":    "pending",
				"createdAt": time.Now(),
			},
			{
				"_id":       bson.NewObjectId(),
				"userId":    bson.NewObjectId(),
				"products":  []bson.ObjectId{bson.NewObjectId()},
				"total":     50.25,
				"status":    "completed",
				"createdAt": time.Now().Add(-24 * time.Hour),
			},
		},
	}
}

// InsertTestData inserts test data into the specified collection
func InsertTestData(t *testing.T, c *mgo.Collection, data []bson.M) {
	for _, doc := range data {
		err := c.Insert(doc)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}
}

// CleanupCollection removes all documents from a collection
func CleanupCollection(t *testing.T, c *mgo.Collection) {
	_, err := c.RemoveAll(bson.M{})
	if err != nil {
		t.Logf("Warning: Failed to cleanup collection: %v", err)
	}
}

// AssertError checks if an error occurred when one was expected
func AssertError(t *testing.T, err error, message string) {
	if err == nil {
		t.Fatalf("Expected error but got none: %s", message)
	}
}

// AssertNoError checks if no error occurred when none was expected
func AssertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("Unexpected error: %s - %v", message, err)
	}
}

// AssertEqual checks if two values are equal
func AssertEqual(t *testing.T, expected, actual interface{}, message string) {
	if expected != actual {
		t.Fatalf("%s - Expected: %v, Got: %v", message, expected, actual)
	}
}

// CreateTestIndex creates an index for testing
func CreateTestIndex(t *testing.T, c *mgo.Collection, key []string, unique bool) {
	index := mgo.Index{
		Key:    key,
		Unique: unique,
	}
	err := c.EnsureIndex(index)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
}
