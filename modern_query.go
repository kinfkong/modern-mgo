// modern_query.go - Query operations for modern MongoDB driver compatibility wrapper

package mgo

import (
	"context"
	"strings"
	"time"

	officialBson "go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// One finds one document (mgo API compatible)
func (q *ModernQ) One(result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	findOpts := &options.FindOneOptions{}
	if q.projection != nil {
		findOpts.Projection = q.projection
	}
	if q.sort != nil {
		findOpts.Sort = q.sort
	}
	if q.skip > 0 {
		findOpts.Skip = &q.skip
	}

	singleResult := q.coll.mgoColl.FindOne(ctx, q.filter, findOpts)
	if singleResult.Err() != nil {
		if singleResult.Err() == mongodrv.ErrNoDocuments {
			return ErrNotFound
		}
		return singleResult.Err()
	}

	var doc officialBson.M
	err := singleResult.Decode(&doc)
	if err != nil {
		return err
	}

	converted := convertOfficialToMGO(doc)
	return mapStructToInterface(converted, result)
}

// All finds all documents
func (q *ModernQ) All(result interface{}) error {
	iter := q.Iter()
	defer iter.Close()
	return iter.All(result)
}

// Count counts query results
func (q *ModernQ) Count() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := &options.CountOptions{}
	if q.skip > 0 {
		opts.Skip = &q.skip
	}
	if q.limit > 0 {
		opts.Limit = &q.limit
	}

	count, err := q.coll.mgoColl.CountDocuments(ctx, q.filter, opts)
	return int(count), err
}

// Iter returns an iterator
func (q *ModernQ) Iter() *ModernIt {
	ctx := context.Background()

	findOpts := &options.FindOptions{}
	if q.projection != nil {
		findOpts.Projection = q.projection
	}
	if q.sort != nil {
		findOpts.Sort = q.sort
	}
	if q.skip > 0 {
		findOpts.Skip = &q.skip
	}
	if q.limit > 0 {
		findOpts.Limit = &q.limit
	}

	cursor, err := q.coll.mgoColl.Find(ctx, q.filter, findOpts)

	return &ModernIt{
		cursor: cursor,
		ctx:    ctx,
		err:    err,
	}
}

// Sort sets sort order
func (q *ModernQ) Sort(fields ...string) *ModernQ {
	var sort officialBson.D
	for _, field := range fields {
		order := 1
		if strings.HasPrefix(field, "-") {
			order = -1
			field = field[1:]
		}
		sort = append(sort, officialBson.E{Key: field, Value: order})
	}
	q.sort = sort
	return q
}

// Limit sets query limit
func (q *ModernQ) Limit(n int) *ModernQ {
	q.limit = int64(n)
	return q
}

// Skip sets query skip
func (q *ModernQ) Skip(n int) *ModernQ {
	q.skip = int64(n)
	return q
}

// Select sets the fields to select (mgo API compatible)
func (q *ModernQ) Select(selector interface{}) *ModernQ {
	q.projection = convertMGOToOfficial(selector)
	return q
}

// Apply applies a change to a single document and returns the old or new document (mgo API compatible)
func (q *ModernQ) Apply(change Change, result interface{}) (*ChangeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var updateDoc interface{}

	if change.Remove {
		// For remove operations, use FindOneAndDelete
		deleteOpts := options.FindOneAndDelete()

		singleResult := q.coll.mgoColl.FindOneAndDelete(ctx, q.filter, deleteOpts)
		if singleResult.Err() != nil {
			if singleResult.Err() == mongodrv.ErrNoDocuments {
				return &ChangeInfo{}, ErrNotFound
			}
			return nil, singleResult.Err()
		}

		if result != nil {
			var doc officialBson.M
			err := singleResult.Decode(&doc)
			if err != nil {
				return nil, err
			}
			converted := convertOfficialToMGO(doc)
			err = mapStructToInterface(converted, result)
			if err != nil {
				return nil, err
			}
		}

		return &ChangeInfo{Removed: 1}, nil
	}

	// For update/upsert operations
	// Wrap plain documents in $set operator for MongoDB compatibility
	wrappedUpdate := wrapInSetOperator(change.Update)
	updateDoc = convertMGOToOfficial(wrappedUpdate)
	updateOpts := options.FindOneAndUpdate()
	updateOpts.SetUpsert(change.Upsert)

	if change.ReturnNew {
		updateOpts.SetReturnDocument(options.After)
	} else {
		updateOpts.SetReturnDocument(options.Before)
	}

	// Track whether this is an upsert that creates a new document
	var wasUpsert bool

	// First, check if the document exists (to determine if it's an upsert)
	if change.Upsert {
		var existingDoc officialBson.M
		findResult := q.coll.mgoColl.FindOne(ctx, q.filter)
		findErr := findResult.Decode(&existingDoc)
		if findErr == mongodrv.ErrNoDocuments {
			wasUpsert = true
		} else if findErr != nil && findErr != mongodrv.ErrNoDocuments {
			// Some other error occurred during the check
			// Continue anyway, as the operation might still succeed
		}
	}

	singleResult := q.coll.mgoColl.FindOneAndUpdate(ctx, q.filter, updateDoc, updateOpts)

	// Handle the case where upsert creates a new document but ReturnDocument is Before
	if singleResult.Err() != nil {
		if singleResult.Err() == mongodrv.ErrNoDocuments {
			if change.Upsert && !change.ReturnNew && wasUpsert {
				// This is expected: upsert created a new doc but we asked for the "before" document
				// We need to get the new document's ID
				// Do a quick find to get the created document
				var newDoc officialBson.M
				findResult := q.coll.mgoColl.FindOne(ctx, q.filter)
				if err := findResult.Decode(&newDoc); err == nil {
					changeInfo := &ChangeInfo{}
					if id, ok := newDoc["_id"]; ok {
						changeInfo.UpsertedId = convertOfficialToMGO(id)
					}
					// If result is requested but ReturnNew is false, we can't populate it
					// because there was no "before" document
					return changeInfo, nil
				}
			}
			return &ChangeInfo{}, ErrNotFound
		}
		return nil, singleResult.Err()
	}

	var doc officialBson.M
	err := singleResult.Decode(&doc)
	if err != nil {
		return nil, err
	}

	converted := convertOfficialToMGO(doc)
	if result != nil {
		err = mapStructToInterface(converted, result)
		if err != nil {
			return nil, err
		}
	}

	changeInfo := &ChangeInfo{}
	if wasUpsert {
		// This was an upsert that created a new document
		if id, ok := doc["_id"]; ok {
			changeInfo.UpsertedId = convertOfficialToMGO(id)
		}
	} else {
		// This was an update of an existing document
		changeInfo.Updated = 1
		changeInfo.Matched = 1
	}

	return changeInfo, nil
}
