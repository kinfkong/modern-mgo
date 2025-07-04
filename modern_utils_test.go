package mgo

import (
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestConvertMGOToOfficialTimeHandling tests time.Time conversion in various contexts
func TestConvertMGOToOfficialTimeHandling(t *testing.T) {
	now := time.Now()

	// Test case 1: Simple time.Time conversion
	result := convertMGOToOfficial(now)
	if _, ok := result.(primitive.DateTime); !ok {
		t.Errorf("Expected primitive.DateTime, got %T", result)
	}

	// Test case 2: Time in bson.M
	doc := bson.M{
		"createdAt": now,
		"updatedAt": now.Add(-1 * time.Hour),
	}
	converted := convertMGOToOfficial(doc).(primitive.M)
	if _, ok := converted["createdAt"].(primitive.DateTime); !ok {
		t.Errorf("Expected createdAt to be primitive.DateTime, got %T", converted["createdAt"])
	}

	// Test case 3: Slice of time.Time
	timeSlice := []time.Time{now, now.Add(-1 * time.Hour), now.Add(-2 * time.Hour)}
	convertedSlice := convertMGOToOfficial(timeSlice).([]interface{})
	for i, item := range convertedSlice {
		if _, ok := item.(primitive.DateTime); !ok {
			t.Errorf("Expected slice item %d to be primitive.DateTime, got %T", i, item)
		}
	}

	// Test case 4: Complex nested structure (like deleteAccount)
	userID := bson.NewObjectId()
	removedData := make(map[string]interface{})

	removedData["elife_accounts"] = []interface{}{
		map[string]interface{}{
			"_id":       userID,
			"createdAt": now,
			"updatedAt": now.Add(-24 * time.Hour),
			"nested": map[string]interface{}{
				"lastLogin": now.Add(-1 * time.Hour),
				"loginHistory": []time.Time{
					now.Add(-3 * time.Hour),
					now.Add(-2 * time.Hour),
				},
			},
		},
	}

	removedData["elife_sessions"] = []map[string]interface{}{
		{
			"_id":       bson.NewObjectId(),
			"userId":    userID,
			"startTime": now.Add(-2 * time.Hour),
			"endTime":   now.Add(-1 * time.Hour),
		},
	}

	complexDoc := bson.M{
		"userId":      userID,
		"removedData": removedData,
		"createdAt":   now,
		"extraInfo": map[string]interface{}{
			"deletionTime": now,
			"metadata": map[string]interface{}{
				"processedAt": now,
			},
		},
	}

	// Convert the complex document
	convertedComplex := convertMGOToOfficial(complexDoc).(primitive.M)

	// Verify createdAt is converted
	if _, ok := convertedComplex["createdAt"].(primitive.DateTime); !ok {
		t.Errorf("Expected root createdAt to be primitive.DateTime, got %T", convertedComplex["createdAt"])
	}

	// Verify nested times are converted
	if removedDataMap, ok := convertedComplex["removedData"].(primitive.M); ok {
		if accounts, ok := removedDataMap["elife_accounts"].([]interface{}); ok && len(accounts) > 0 {
			if account, ok := accounts[0].(primitive.M); ok {
				if _, ok := account["createdAt"].(primitive.DateTime); !ok {
					t.Errorf("Expected nested createdAt to be primitive.DateTime, got %T", account["createdAt"])
				}

				// Check deeply nested times
				if nested, ok := account["nested"].(primitive.M); ok {
					if _, ok := nested["lastLogin"].(primitive.DateTime); !ok {
						t.Errorf("Expected deeply nested lastLogin to be primitive.DateTime, got %T", nested["lastLogin"])
					}

					// Check time slice in nested structure
					if history, ok := nested["loginHistory"].([]interface{}); ok {
						for i, timeVal := range history {
							if _, ok := timeVal.(primitive.DateTime); !ok {
								t.Errorf("Expected loginHistory[%d] to be primitive.DateTime, got %T", i, timeVal)
							}
						}
					}
				}
			}
		}

		// Check slice of maps conversion
		if sessions, ok := removedDataMap["elife_sessions"].([]interface{}); ok && len(sessions) > 0 {
			if session, ok := sessions[0].(primitive.M); ok {
				if _, ok := session["startTime"].(primitive.DateTime); !ok {
					t.Errorf("Expected session startTime to be primitive.DateTime, got %T", session["startTime"])
				}
			}
		}
	}

	// Test case 5: Nil and zero time values
	docWithNils := bson.M{
		"nilTime":     (*time.Time)(nil),
		"zeroTime":    time.Time{},
		"validTime":   now,
		"timePointer": &now,
	}

	convertedNils := convertMGOToOfficial(docWithNils).(primitive.M)

	// Nil should remain nil
	if convertedNils["nilTime"] != nil {
		t.Errorf("Expected nil time to remain nil, got %v", convertedNils["nilTime"])
	}

	// Zero time should be converted to DateTime
	if _, ok := convertedNils["zeroTime"].(primitive.DateTime); !ok {
		t.Errorf("Expected zero time to be primitive.DateTime, got %T", convertedNils["zeroTime"])
	}

	// Time pointer should be dereferenced and converted
	if _, ok := convertedNils["timePointer"].(primitive.DateTime); !ok {
		t.Errorf("Expected time pointer to be converted to primitive.DateTime, got %T", convertedNils["timePointer"])
	}
}

// TestConvertMGOToOfficialEdgeCases tests edge cases in conversion
func TestConvertMGOToOfficialEdgeCases(t *testing.T) {
	// Test empty slices
	emptyTimeSlice := []time.Time{}
	converted := convertMGOToOfficial(emptyTimeSlice)
	if slice, ok := converted.([]interface{}); !ok || len(slice) != 0 {
		t.Errorf("Expected empty slice to remain empty, got %v", converted)
	}

	// Test mixed type slice
	mixedSlice := []interface{}{
		time.Now(),
		"string",
		123,
		bson.M{"key": "value"},
		[]time.Time{time.Now()},
	}

	convertedMixed := convertMGOToOfficial(mixedSlice).([]interface{})

	// First item should be DateTime
	if _, ok := convertedMixed[0].(primitive.DateTime); !ok {
		t.Errorf("Expected first item to be primitive.DateTime, got %T", convertedMixed[0])
	}

	// String should remain string
	if _, ok := convertedMixed[1].(string); !ok {
		t.Errorf("Expected second item to remain string, got %T", convertedMixed[1])
	}

	// Nested slice of times should be converted
	if nestedSlice, ok := convertedMixed[4].([]interface{}); ok && len(nestedSlice) > 0 {
		if _, ok := nestedSlice[0].(primitive.DateTime); !ok {
			t.Errorf("Expected nested time to be primitive.DateTime, got %T", nestedSlice[0])
		}
	}
}

// TestConvertMGOToOfficialDeleteAccountScenario tests the exact scenario from deleteAccount
// that was causing "Badly formed input data" error
func TestConvertMGOToOfficialDeleteAccountScenario(t *testing.T) {
	userID := bson.NewObjectId()
	now := time.Now()

	// Simulate the exact structure from deleteAccount
	removedData := make(map[string]interface{})

	// Various collection data structures that might be in removedData
	removedData["collection1"] = []interface{}{
		bson.M{
			"_id":       bson.NewObjectId(),
			"userId":    userID,
			"createdAt": now,
			"data":      "some data",
		},
	}

	removedData["collection2"] = []bson.M{
		{
			"_id":       bson.NewObjectId(),
			"userId":    userID,
			"timestamp": now,
		},
	}

	removedData["collection3"] = []map[string]interface{}{
		{
			"_id":    bson.NewObjectId(),
			"userId": userID,
			"times": map[string]time.Time{
				"start": now.Add(-1 * time.Hour),
				"end":   now,
			},
		},
	}

	// Empty collections
	removedData["emptyCollection"] = []interface{}{}
	removedData["nilCollection"] = nil

	// Complex nested structure
	removedData["complexCollection"] = []interface{}{
		map[string]interface{}{
			"_id": bson.NewObjectId(),
			"nested": map[string]interface{}{
				"deep": map[string]interface{}{
					"times": []time.Time{now, now.Add(1 * time.Hour)},
					"data": map[string]interface{}{
						"createdAt": now,
						"items": []interface{}{
							map[string]interface{}{
								"timestamp": now,
								"value":     123,
							},
						},
					},
				},
			},
		},
	}

	// Extra info with various data types
	extraInfo := map[string]interface{}{
		"reason":     "user_request",
		"timestamp":  now,
		"adminId":    bson.NewObjectId(),
		"metadata":   nil,
		"flags":      []string{"deleted", "archived"},
		"retryCount": 3,
		"success":    true,
		"details": map[string]interface{}{
			"ip":        "192.168.1.1",
			"userAgent": "Mozilla/5.0",
			"duration":  1.5,
		},
	}

	// Create the exact document structure
	doc := bson.M{
		"userId":      userID,
		"removedData": removedData,
		"createdAt":   time.Now(), // Fresh time.Now() call like in the error
		"extraInfo":   extraInfo,
	}

	// Convert the document
	converted := convertMGOToOfficial(doc)

	// Verify the conversion doesn't panic and returns a valid structure
	if converted == nil {
		t.Fatal("Conversion returned nil")
	}

	// Verify it's converted to primitive.M
	convertedDoc, ok := converted.(primitive.M)
	if !ok {
		t.Fatalf("Expected primitive.M, got %T", converted)
	}

	// Verify userId is converted to ObjectID
	if uid, ok := convertedDoc["userId"].(primitive.ObjectID); !ok {
		t.Errorf("Expected userId to be primitive.ObjectID, got %T", convertedDoc["userId"])
	} else if len(uid) != 12 {
		t.Errorf("Invalid ObjectID length: %d", len(uid))
	}

	// Verify createdAt is converted to DateTime
	if _, ok := convertedDoc["createdAt"].(primitive.DateTime); !ok {
		t.Errorf("Expected createdAt to be primitive.DateTime, got %T", convertedDoc["createdAt"])
	}

	// Verify removedData structure
	if rd, ok := convertedDoc["removedData"].(primitive.M); ok {
		// Check various collection formats
		if col1, ok := rd["collection1"].([]interface{}); ok && len(col1) > 0 {
			if item, ok := col1[0].(primitive.M); ok {
				if _, ok := item["createdAt"].(primitive.DateTime); !ok {
					t.Errorf("Expected collection1 item createdAt to be primitive.DateTime, got %T", item["createdAt"])
				}
			}
		}

		// Check empty collection
		if empty, ok := rd["emptyCollection"].([]interface{}); !ok || len(empty) != 0 {
			t.Error("Empty collection should remain empty")
		}

		// Check nil collection
		if rd["nilCollection"] != nil {
			t.Error("Nil collection should remain nil")
		}

		// Check complex nested structure
		if complex, ok := rd["complexCollection"].([]interface{}); ok && len(complex) > 0 {
			if item, ok := complex[0].(primitive.M); ok {
				if nested, ok := item["nested"].(primitive.M); ok {
					if deep, ok := nested["deep"].(primitive.M); ok {
						// Check nested time slice
						if times, ok := deep["times"].([]interface{}); ok {
							for i, tm := range times {
								if _, ok := tm.(primitive.DateTime); !ok {
									t.Errorf("Expected nested time[%d] to be primitive.DateTime, got %T", i, tm)
								}
							}
						}
					}
				}
			}
		}
	}

	// Verify extraInfo structure
	if ei, ok := convertedDoc["extraInfo"].(primitive.M); ok {
		// Check timestamp conversion
		if _, ok := ei["timestamp"].(primitive.DateTime); !ok {
			t.Errorf("Expected extraInfo timestamp to be primitive.DateTime, got %T", ei["timestamp"])
		}

		// Check ObjectId conversion
		if adminId, ok := ei["adminId"].(primitive.ObjectID); !ok {
			t.Errorf("Expected adminId to be primitive.ObjectID, got %T", ei["adminId"])
		} else if len(adminId) != 12 {
			t.Errorf("Invalid adminId ObjectID length: %d", len(adminId))
		}

		// Check nil handling
		if ei["metadata"] != nil {
			t.Error("Nil metadata should remain nil")
		}
	}

	// Final validation: ensure the converted document can be used with MongoDB driver
	// This is what would cause "Badly formed input data" if conversion is incorrect
	// We're just checking that the structure is valid, not actually inserting
	if _, err := bson.Marshal(convertedDoc); err != nil {
		t.Errorf("Converted document cannot be marshaled to BSON: %v", err)
	}
}
