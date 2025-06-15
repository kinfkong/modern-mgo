// modern_types.go - Type definitions for modern MongoDB driver compatibility wrapper

package mgo

import (
	"context"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ModernMGO provides the mgo API using the official MongoDB driver
type ModernMGO struct {
	client     *mongodrv.Client
	dbName     string
	mode       Mode
	safe       *Safe
	isOriginal bool // Track if this is the original session or a copy
}

// ModernDB wraps the modern database
type ModernDB struct {
	mgoDB *mongodrv.Database
	name  string
}

// ModernColl wraps the modern collection
type ModernColl struct {
	mgoColl *mongodrv.Collection
	name    string
}

// ModernQ wraps query state
type ModernQ struct {
	coll       *ModernColl
	filter     interface{}
	sort       interface{}
	skip       int64
	limit      int64
	projection interface{}
}

// ModernIt wraps cursor iteration
type ModernIt struct {
	cursor *mongodrv.Cursor
	ctx    context.Context
	err    error
}

// ModernPipe wraps aggregation pipeline state
type ModernPipe struct {
	collection *ModernColl
	pipeline   interface{}
	allowDisk  bool
	batchSize  int32
	maxTimeMS  int64
	collation  *options.Collation
}

// ModernBulk provides bulk operations using the official MongoDB driver
type ModernBulk struct {
	collection *ModernColl
	operations []mongodrv.WriteModel
	ordered    bool
	opcount    int
}

// ModernGridFS provides GridFS operations using the official MongoDB driver
type ModernGridFS struct {
	Files  *ModernColl
	Chunks *ModernColl
	prefix string
}

// ModernGridFile wraps GridFS file operations
type ModernGridFile struct {
	id          interface{}
	filename    string
	contentType string
	chunkSize   int
	length      int64
	md5         string
	uploadDate  time.Time
	metadata    interface{}
	gfs         *ModernGridFS
	chunks      [][]byte
	closed      bool
}
