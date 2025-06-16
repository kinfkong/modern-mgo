// modern_aggregation.go - Aggregation pipeline operations for modern MongoDB driver compatibility wrapper

package mgo

import (
	"context"
	"time"

	"github.com/globalsign/mgo/bson"
	officialBson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Iter executes the aggregation pipeline and returns an iterator
func (p *ModernPipe) Iter() *ModernIt {
	ctx := context.Background()

	// Convert pipeline to the correct format for the official driver
	var pipeline interface{}

	// Handle different pipeline input types
	switch v := p.pipeline.(type) {
	case []interface{}:
		// Already converted, use as-is
		pipeline = v
	case []bson.M:
		// Convert []bson.M to []interface{}
		converted := make([]interface{}, len(v))
		for i, stage := range v {
			converted[i] = convertMGOToOfficial(stage)
		}
		pipeline = converted
	case []officialBson.M:
		// Already in official format
		pipeline = v
	default:
		// Try to convert single stage
		pipeline = []interface{}{convertMGOToOfficial(v)}
	}

	// Create aggregation options
	opts := &options.AggregateOptions{}
	if p.allowDisk {
		opts.AllowDiskUse = &p.allowDisk
	}
	if p.batchSize > 0 {
		opts.BatchSize = &p.batchSize
	}
	if p.maxTimeMS > 0 {
		maxTime := time.Duration(p.maxTimeMS) * time.Millisecond
		opts.MaxTime = &maxTime
	}
	if p.collation != nil {
		opts.Collation = p.collation
	}

	cursor, err := p.collection.mgoColl.Aggregate(ctx, pipeline, opts)

	return &ModernIt{
		cursor: cursor,
		ctx:    ctx,
		err:    err,
	}
}

// All executes the pipeline and returns all results
func (p *ModernPipe) All(result interface{}) error {
	iter := p.Iter()
	defer iter.Close()
	return iter.All(result)
}

// One executes the pipeline and returns the first result
func (p *ModernPipe) One(result interface{}) error {
	iter := p.Iter()
	defer iter.Close()

	if iter.Next(result) {
		return nil
	}
	if err := iter.err; err != nil {
		return err
	}
	return ErrNotFound
}

// Explain returns aggregation execution statistics
func (p *ModernPipe) Explain(result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Convert pipeline to the correct format
	var pipeline []interface{}

	switch v := p.pipeline.(type) {
	case []interface{}:
		pipeline = v
	case []bson.M:
		pipeline = make([]interface{}, len(v))
		for i, stage := range v {
			pipeline[i] = convertMGOToOfficial(stage)
		}
	case []officialBson.M:
		pipeline = make([]interface{}, len(v))
		for i, stage := range v {
			pipeline[i] = stage
		}
	default:
		pipeline = []interface{}{convertMGOToOfficial(v)}
	}

	// Create explain command
	explainCmd := officialBson.M{
		"aggregate": p.collection.name,
		"pipeline":  pipeline,
		"explain":   true,
	}

	db := p.collection.mgoColl.Database()
	singleResult := db.RunCommand(ctx, explainCmd)

	var doc officialBson.M
	err := singleResult.Decode(&doc)
	if err != nil {
		return err
	}

	converted := convertOfficialToMGO(doc)
	return mapStructToInterface(converted, result)
}

// AllowDiskUse enables writing to temporary files during aggregation
func (p *ModernPipe) AllowDiskUse() *ModernPipe {
	p.allowDisk = true
	return p
}

// Batch sets the batch size for the aggregation cursor
func (p *ModernPipe) Batch(n int) *ModernPipe {
	p.batchSize = int32(n)
	return p
}

// SetMaxTime sets the maximum execution time for the aggregation
func (p *ModernPipe) SetMaxTime(d time.Duration) *ModernPipe {
	p.maxTimeMS = int64(d / time.Millisecond)
	return p
}

// Collation sets the collation for the aggregation
func (p *ModernPipe) Collation(collation *Collation) *ModernPipe {
	if collation != nil {
		// Convert mgo Collation to official driver Collation
		p.collation = &options.Collation{
			Locale:          collation.Locale,
			CaseFirst:       collation.CaseFirst,
			Strength:        collation.Strength,
			Alternate:       collation.Alternate,
			MaxVariable:     collation.MaxVariable,
			Normalization:   collation.Normalization,
			CaseLevel:       collation.CaseLevel,
			NumericOrdering: collation.NumericOrdering,
			Backwards:       collation.Backwards,
		}
	}
	return p
}
