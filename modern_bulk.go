package mgo

import (
	"context"
	"time"

	"github.com/kinfkong/modern-mgo/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// -------------------- Bulk operations --------------------

// Unordered puts the bulk operation in unordered mode (mgo API compatible)
func (b *ModernBulk) Unordered() {
	b.ordered = false
}

// Insert queues up documents for insertion (mgo API compatible)
func (b *ModernBulk) Insert(docs ...interface{}) {
	for _, doc := range docs {
		convertedDoc := convertMGOToOfficial(doc)
		insertModel := mongodrv.NewInsertOneModel().SetDocument(convertedDoc)
		b.operations = append(b.operations, insertModel)
		b.opcount++
	}
}

// Update queues up pairs of updating instructions (mgo API compatible)
// Each pair matches exactly one document for updating at most
func (b *ModernBulk) Update(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.Update requires an even number of parameters")
	}

	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		update := pairs[i+1]

		if selector == nil {
			selector = bson.D{}
		}

		filter := convertMGOToOfficial(selector)
		updateDoc := convertMGOToOfficial(update)

		updateModel := mongodrv.NewUpdateOneModel().SetFilter(filter).SetUpdate(updateDoc)
		b.operations = append(b.operations, updateModel)
		b.opcount++
	}
}

// UpdateAll queues up pairs of updating instructions (mgo API compatible)
// Each pair updates all documents matching the selector
func (b *ModernBulk) UpdateAll(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.UpdateAll requires an even number of parameters")
	}

	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		update := pairs[i+1]

		if selector == nil {
			selector = bson.D{}
		}

		filter := convertMGOToOfficial(selector)
		updateDoc := convertMGOToOfficial(update)

		updateModel := mongodrv.NewUpdateManyModel().SetFilter(filter).SetUpdate(updateDoc)
		b.operations = append(b.operations, updateModel)
		b.opcount++
	}
}

// Upsert queues up pairs of upserting instructions (mgo API compatible)
// Each pair matches exactly one document for updating at most
func (b *ModernBulk) Upsert(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.Upsert requires an even number of parameters")
	}

	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		update := pairs[i+1]

		if selector == nil {
			selector = bson.D{}
		}

		filter := convertMGOToOfficial(selector)
		updateDoc := convertMGOToOfficial(update)

		upsert := true
		updateModel := mongodrv.NewUpdateOneModel().SetFilter(filter).SetUpdate(updateDoc).SetUpsert(upsert)
		b.operations = append(b.operations, updateModel)
		b.opcount++
	}
}

// Remove queues up selectors for removing matching documents (mgo API compatible)
// Each selector will remove only a single matching document
func (b *ModernBulk) Remove(selectors ...interface{}) {
	for _, selector := range selectors {
		if selector == nil {
			selector = bson.D{}
		}

		filter := convertMGOToOfficial(selector)
		deleteModel := mongodrv.NewDeleteOneModel().SetFilter(filter)
		b.operations = append(b.operations, deleteModel)
		b.opcount++
	}
}

// RemoveAll queues up selectors for removing all matching documents (mgo API compatible)
// Each selector will remove all matching documents
func (b *ModernBulk) RemoveAll(selectors ...interface{}) {
	for _, selector := range selectors {
		if selector == nil {
			selector = bson.D{}
		}

		filter := convertMGOToOfficial(selector)
		deleteModel := mongodrv.NewDeleteManyModel().SetFilter(filter)
		b.operations = append(b.operations, deleteModel)
		b.opcount++
	}
}

// Run executes all queued bulk operations (mgo API compatible)
func (b *ModernBulk) Run() (*BulkResult, error) {
	if len(b.operations) == 0 {
		return &BulkResult{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := options.BulkWrite().SetOrdered(b.ordered)

	result, err := b.collection.mgoColl.BulkWrite(ctx, b.operations, opts)
	if err != nil {
		// Convert bulk write errors to mgo format
		if bulkErr, ok := err.(mongodrv.BulkWriteException); ok {
			return b.convertBulkError(result, &bulkErr)
		}
		return nil, err
	}

	return b.convertBulkResult(result), nil
}

// convertBulkResult converts official driver BulkWriteResult to mgo BulkResult
func (b *ModernBulk) convertBulkResult(result *mongodrv.BulkWriteResult) *BulkResult {
	if result == nil {
		return &BulkResult{}
	}

	// For delete operations, DeletedCount represents both matched and modified
	// For update operations, use MatchedCount and ModifiedCount
	matched := int(result.MatchedCount + result.DeletedCount)
	modified := int(result.ModifiedCount + result.DeletedCount + result.UpsertedCount)

	return &BulkResult{
		Matched:  matched,
		Modified: modified,
	}
}

// convertBulkError converts official driver BulkWriteException to mgo BulkError
func (b *ModernBulk) convertBulkError(result *mongodrv.BulkWriteResult, bulkErr *mongodrv.BulkWriteException) (*BulkResult, error) {
	// Convert write errors to BulkErrorCase format
	var ecases []BulkErrorCase

	for _, writeErr := range bulkErr.WriteErrors {
		ecase := BulkErrorCase{
			Index: writeErr.Index,
			Err: &QueryError{
				Code:    writeErr.Code,
				Message: writeErr.Message,
			},
		}
		ecases = append(ecases, ecase)
	}

	// Handle write concern error if present
	if bulkErr.WriteConcernError != nil {
		ecase := BulkErrorCase{
			Index: -1, // Write concern errors don't have specific indices
			Err: &QueryError{
				Code:    bulkErr.WriteConcernError.Code,
				Message: bulkErr.WriteConcernError.Message,
			},
		}
		ecases = append(ecases, ecase)
	}

	bulkResult := b.convertBulkResult(result)

	if len(ecases) > 0 {
		return bulkResult, &BulkError{ecases: ecases}
	}

	// If we have a bulk write exception but no specific errors, return the general error
	return bulkResult, &BulkError{
		ecases: []BulkErrorCase{{
			Index: -1,
			Err: &QueryError{
				Message: bulkErr.Error(),
			},
		}},
	}
}
