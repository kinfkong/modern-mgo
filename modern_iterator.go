// modern_iterator.go - Iterator operations for modern MongoDB driver compatibility wrapper

package mgo

import (
	"github.com/globalsign/mgo/bson"
	officialBson "go.mongodb.org/mongo-driver/bson"
)

// Next gets next document from iterator
func (it *ModernIt) Next(result interface{}) bool {
	if it.err != nil {
		return false
	}

	if it.cursor == nil {
		it.err = ErrNotFound
		return false
	}

	if !it.cursor.Next(it.ctx) {
		// Check if there was an actual error, or just end of cursor
		it.err = it.cursor.Err()
		// Don't set ErrNotFound here - end of iteration is normal
		return false
	}

	var doc officialBson.M
	err := it.cursor.Decode(&doc)
	if err != nil {
		it.err = err
		return false
	}

	converted := convertOfficialToMGO(doc)
	it.err = mapStructToInterface(converted, result)
	return it.err == nil
}

// Close closes the iterator
func (it *ModernIt) Close() error {
	if it.cursor != nil {
		err := it.cursor.Close(it.ctx)
		if err != nil && it.err == nil {
			it.err = err
		}
	}
	return it.err
}

// All gets all documents from iterator
func (it *ModernIt) All(result interface{}) error {
	if it.err != nil {
		return it.err
	}

	if it.cursor == nil {
		return ErrNotFound
	}

	// Use Next() in a loop to avoid BSON slice unmarshalling issues
	var docs []interface{}

	for {
		var doc bson.M
		if !it.Next(&doc) {
			break
		}
		if it.err != nil {
			return it.err
		}
		docs = append(docs, doc)
	}

	// Check for iteration errors (not end-of-cursor)
	if it.err != nil && it.err != ErrNotFound {
		return it.err
	}

	// Reset error since reaching end of cursor is expected
	it.err = nil

	return mapStructToInterface(docs, result)
}
