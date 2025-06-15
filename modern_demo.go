// modern_demo.go - Working demonstration of MongoDB modern driver compatibility wrapper
// This shows how to maintain the mgo API while using the official MongoDB driver

package mgo

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/kinfkong/modern-mgo/bson"
	officialBson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Session operations moved to modern_session.go
// Database operations moved to modern_session.go
// Collection operations moved to modern_collection.go

// Query operations moved to modern_query.go

// Iterator operations moved to modern_iterator.go

// Aggregation operations moved to modern_aggregation.go

// Run and Bulk operations moved to modern_collection.go

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

// GridFS Operations Implementation

// Create creates a new GridFS file for writing (mgo API compatible)
func (gfs *ModernGridFS) Create(filename string) (*ModernGridFile, error) {
	return &ModernGridFile{
		id:          bson.NewObjectId(),
		filename:    filename,
		contentType: "",
		chunkSize:   255 * 1024, // Default chunk size
		length:      0,
		uploadDate:  time.Now(),
		gfs:         gfs,
		chunks:      make([][]byte, 0),
		closed:      false,
	}, nil
}

// Open opens the most recent GridFS file with the given filename for reading (mgo API compatible)
func (gfs *ModernGridFS) Open(filename string) (*ModernGridFile, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Find the most recent file with this filename
	filter := convertMGOToOfficial(bson.M{"filename": filename})
	// Use officialBson.D to ensure proper sort order
	opts := options.FindOne().SetSort(officialBson.D{{"uploadDate", -1}})

	var fileDoc bson.M
	err := gfs.Files.mgoColl.FindOne(ctx, filter, opts).Decode(&fileDoc)
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	// Convert the document to a ModernGridFile
	file := &ModernGridFile{
		gfs:    gfs,
		closed: false,
	}

	if id, ok := fileDoc["_id"]; ok {
		file.id = id
	}
	if fn, ok := fileDoc["filename"].(string); ok {
		file.filename = fn
	}
	if ct, ok := fileDoc["contentType"].(string); ok {
		file.contentType = ct
	}
	if cs, ok := fileDoc["chunkSize"].(int32); ok {
		file.chunkSize = int(cs)
	} else if cs, ok := fileDoc["chunkSize"].(int); ok {
		file.chunkSize = cs
	}
	if length, ok := fileDoc["length"].(int64); ok {
		file.length = length
	} else if length, ok := fileDoc["length"].(int32); ok {
		file.length = int64(length)
	}
	if md5, ok := fileDoc["md5"].(string); ok {
		file.md5 = md5
	}
	if ud, ok := fileDoc["uploadDate"].(time.Time); ok {
		file.uploadDate = ud
	}
	if metadata, ok := fileDoc["metadata"]; ok {
		file.metadata = metadata
	}

	return file, nil
}

// OpenId opens a GridFS file by its ID for reading (mgo API compatible)
func (gfs *ModernGridFS) OpenId(id interface{}) (*ModernGridFile, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := convertMGOToOfficial(bson.M{"_id": id})
	var fileDoc bson.M
	err := gfs.Files.mgoColl.FindOne(ctx, filter).Decode(&fileDoc)
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	// Convert the document to a ModernGridFile
	file := &ModernGridFile{
		gfs:    gfs,
		closed: false,
	}

	if id, ok := fileDoc["_id"]; ok {
		file.id = id
	}
	if fn, ok := fileDoc["filename"].(string); ok {
		file.filename = fn
	}
	if ct, ok := fileDoc["contentType"].(string); ok {
		file.contentType = ct
	}
	if cs, ok := fileDoc["chunkSize"].(int32); ok {
		file.chunkSize = int(cs)
	} else if cs, ok := fileDoc["chunkSize"].(int); ok {
		file.chunkSize = cs
	}
	if length, ok := fileDoc["length"].(int64); ok {
		file.length = length
	} else if length, ok := fileDoc["length"].(int32); ok {
		file.length = int64(length)
	}
	if md5, ok := fileDoc["md5"].(string); ok {
		file.md5 = md5
	}
	if ud, ok := fileDoc["uploadDate"].(time.Time); ok {
		file.uploadDate = ud
	}
	if metadata, ok := fileDoc["metadata"]; ok {
		file.metadata = metadata
	}

	return file, nil
}

// Remove removes all GridFS files with the given filename (mgo API compatible)
func (gfs *ModernGridFS) Remove(filename string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Find all files with this filename to get their IDs
	filter := convertMGOToOfficial(bson.M{"filename": filename})
	cursor, err := gfs.Files.mgoColl.Find(ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var fileIds []interface{}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		if id, ok := doc["_id"]; ok {
			fileIds = append(fileIds, id)
		}
	}

	// Remove the files and chunks
	for _, id := range fileIds {
		if err := gfs.RemoveId(id); err != nil {
			return err
		}
	}

	return nil
}

// RemoveId removes a GridFS file by its ID (mgo API compatible)
func (gfs *ModernGridFS) RemoveId(id interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Remove the file document
	fileFilter := convertMGOToOfficial(bson.M{"_id": id})
	_, err := gfs.Files.mgoColl.DeleteOne(ctx, fileFilter)
	if err != nil {
		return err
	}

	// Remove the chunks
	chunkFilter := convertMGOToOfficial(bson.M{"files_id": id})
	_, err = gfs.Chunks.mgoColl.DeleteMany(ctx, chunkFilter)
	return err
}

// Find returns a query for finding GridFS files (mgo API compatible)
func (gfs *ModernGridFS) Find(selector interface{}) *ModernQ {
	return gfs.Files.Find(selector)
}

// OpenNext opens the next file from an iterator (mgo API compatible)
func (gfs *ModernGridFS) OpenNext(iter *ModernIt, file **ModernGridFile) bool {
	if *file != nil {
		(*file).Close()
	}

	var fileDoc bson.M
	if !iter.Next(&fileDoc) {
		*file = nil
		return false
	}

	// Convert document to ModernGridFile
	f := &ModernGridFile{
		gfs:    gfs,
		closed: false,
	}

	if id, ok := fileDoc["_id"]; ok {
		f.id = id
	}
	if fn, ok := fileDoc["filename"].(string); ok {
		f.filename = fn
	}
	if ct, ok := fileDoc["contentType"].(string); ok {
		f.contentType = ct
	}
	if cs, ok := fileDoc["chunkSize"].(int32); ok {
		f.chunkSize = int(cs)
	} else if cs, ok := fileDoc["chunkSize"].(int); ok {
		f.chunkSize = cs
	}
	if length, ok := fileDoc["length"].(int64); ok {
		f.length = length
	} else if length, ok := fileDoc["length"].(int32); ok {
		f.length = int64(length)
	}
	if md5, ok := fileDoc["md5"].(string); ok {
		f.md5 = md5
	}
	if ud, ok := fileDoc["uploadDate"].(time.Time); ok {
		f.uploadDate = ud
	}
	if metadata, ok := fileDoc["metadata"]; ok {
		f.metadata = metadata
	}

	*file = f
	return true
}

// GridFile Operations Implementation

// Write writes data to the GridFS file (mgo API compatible)
func (f *ModernGridFile) Write(data []byte) (int, error) {
	if f.closed {
		return 0, errors.New("file is closed")
	}

	f.chunks = append(f.chunks, data)
	f.length += int64(len(data))
	return len(data), nil
}

// Read reads data from the GridFS file (mgo API compatible)
func (f *ModernGridFile) Read(data []byte) (int, error) {
	if f.closed {
		return 0, errors.New("file is closed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Load chunks if not loaded
	if f.chunks == nil {
		filter := convertMGOToOfficial(bson.M{"files_id": f.id})
		// Use officialBson.D to ensure proper sort order
		opts := options.Find().SetSort(officialBson.D{{"n", 1}})

		cursor, err := f.gfs.Chunks.mgoColl.Find(ctx, filter, opts)
		if err != nil {
			return 0, err
		}
		defer cursor.Close(ctx)

		f.chunks = make([][]byte, 0)
		for cursor.Next(ctx) {
			var chunkDoc bson.M
			if err := cursor.Decode(&chunkDoc); err != nil {
				continue
			}

			// Handle different data types returned by MongoDB driver
			var chunkData []byte
			switch data := chunkDoc["data"].(type) {
			case []byte:
				chunkData = data
			case primitive.Binary:
				chunkData = data.Data
			default:
				// Try to convert to []byte if possible
				continue
			}

			if len(chunkData) > 0 {
				f.chunks = append(f.chunks, chunkData)
			}
		}
	}

	// Read from chunks
	totalRead := 0
	for _, chunk := range f.chunks {
		if totalRead >= len(data) {
			break
		}
		n := copy(data[totalRead:], chunk)
		totalRead += n
		if n < len(chunk) {
			break
		}
	}

	if totalRead == 0 {
		return 0, io.EOF
	}

	return totalRead, nil
}

// Close closes the GridFS file (mgo API compatible)
func (f *ModernGridFile) Close() error {
	if f.closed {
		return nil
	}

	// If this is a write operation, save the file
	if len(f.chunks) > 0 {
		if err := f.saveFile(); err != nil {
			return err
		}
	}

	f.closed = true
	return nil
}

// saveFile saves the GridFS file and its chunks to MongoDB
func (f *ModernGridFile) saveFile() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Calculate MD5 hash
	hasher := md5.New()
	for _, chunk := range f.chunks {
		hasher.Write(chunk)
	}
	f.md5 = fmt.Sprintf("%x", hasher.Sum(nil))

	// Save the file document
	fileDoc := bson.M{
		"_id":         f.id,
		"filename":    f.filename,
		"contentType": f.contentType,
		"length":      f.length,
		"chunkSize":   f.chunkSize,
		"uploadDate":  f.uploadDate,
		"md5":         f.md5,
	}

	if f.metadata != nil {
		fileDoc["metadata"] = f.metadata
	}

	_, err := f.gfs.Files.mgoColl.InsertOne(ctx, convertMGOToOfficial(fileDoc))
	if err != nil {
		return err
	}

	// Save chunks
	for i, chunkData := range f.chunks {
		chunkDoc := bson.M{
			"_id":      bson.NewObjectId(),
			"files_id": f.id,
			"n":        i,
			"data":     chunkData,
		}

		_, err := f.gfs.Chunks.mgoColl.InsertOne(ctx, convertMGOToOfficial(chunkDoc))
		if err != nil {
			return err
		}
	}

	// Ensure index on chunks collection
	return f.gfs.Chunks.EnsureIndex(Index{
		Key:    []string{"files_id", "n"},
		Unique: true,
	})
}

// GridFile property getters and setters (mgo API compatible)

// Id returns the file ID
func (f *ModernGridFile) Id() interface{} {
	return f.id
}

// SetId sets the file ID
func (f *ModernGridFile) SetId(id interface{}) {
	f.id = id
}

// Name returns the filename
func (f *ModernGridFile) Name() string {
	return f.filename
}

// SetName sets the filename
func (f *ModernGridFile) SetName(filename string) {
	f.filename = filename
}

// ContentType returns the content type
func (f *ModernGridFile) ContentType() string {
	return f.contentType
}

// SetContentType sets the content type
func (f *ModernGridFile) SetContentType(contentType string) {
	f.contentType = contentType
}

// Size returns the file size
func (f *ModernGridFile) Size() int64 {
	return f.length
}

// MD5 returns the MD5 hash
func (f *ModernGridFile) MD5() string {
	return f.md5
}

// UploadDate returns the upload date
func (f *ModernGridFile) UploadDate() time.Time {
	return f.uploadDate
}

// SetUploadDate sets the upload date
func (f *ModernGridFile) SetUploadDate(t time.Time) {
	f.uploadDate = t
}

// GetMeta gets the metadata
func (f *ModernGridFile) GetMeta(result interface{}) error {
	if f.metadata == nil {
		return nil
	}
	return mapStructToInterface(f.metadata, result)
}

// SetMeta sets the metadata
func (f *ModernGridFile) SetMeta(metadata interface{}) {
	f.metadata = metadata
}

// SetChunkSize sets the chunk size
func (f *ModernGridFile) SetChunkSize(size int) {
	f.chunkSize = size
}

// Additional session methods moved to modern_session.go
// Additional collection methods moved to modern_collection.go
