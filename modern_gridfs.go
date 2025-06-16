package mgo

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/globalsign/mgo/bson"
	officialBson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// -------------------- GridFS operations --------------------

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

	filter := convertMGOToOfficial(bson.M{"filename": filename})
	opts := options.FindOne().SetSort(officialBson.D{{Key: "uploadDate", Value: -1}})

	var fileDoc bson.M
	err := gfs.Files.mgoColl.FindOne(ctx, filter, opts).Decode(&fileDoc)
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

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
	if md5str, ok := fileDoc["md5"].(string); ok {
		file.md5 = md5str
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
	if md5str, ok := fileDoc["md5"].(string); ok {
		file.md5 = md5str
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

	filter := convertMGOToOfficial(bson.M{"filename": filename})
	cursor, err := gfs.Files.mgoColl.Find(ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var ids []interface{}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		if id, ok := doc["_id"]; ok {
			ids = append(ids, id)
		}
	}

	for _, id := range ids {
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

	fileFilter := convertMGOToOfficial(bson.M{"_id": id})
	if _, err := gfs.Files.mgoColl.DeleteOne(ctx, fileFilter); err != nil {
		return err
	}

	chunkFilter := convertMGOToOfficial(bson.M{"files_id": id})
	_, err := gfs.Chunks.mgoColl.DeleteMany(ctx, chunkFilter)
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
	if md5str, ok := fileDoc["md5"].(string); ok {
		f.md5 = md5str
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

// -------------------- GridFile operations --------------------

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

	if f.chunks == nil {
		filter := convertMGOToOfficial(bson.M{"files_id": f.id})
		opts := options.Find().SetSort(officialBson.D{{Key: "n", Value: 1}})

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

			var chunkData []byte
			switch dt := chunkDoc["data"].(type) {
			case []byte:
				chunkData = dt
			case primitive.Binary:
				chunkData = dt.Data
			default:
				continue
			}

			if len(chunkData) > 0 {
				f.chunks = append(f.chunks, chunkData)
			}
		}
	}

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

	if len(f.chunks) > 0 {
		if err := f.saveFile(); err != nil {
			return err
		}
	}

	f.closed = true
	return nil
}

// saveFile persists the GridFS file and its chunks to MongoDB
func (f *ModernGridFile) saveFile() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	hasher := md5.New()
	for _, chunk := range f.chunks {
		hasher.Write(chunk)
	}
	f.md5 = fmt.Sprintf("%x", hasher.Sum(nil))

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

	if _, err := f.gfs.Files.mgoColl.InsertOne(ctx, convertMGOToOfficial(fileDoc)); err != nil {
		return err
	}

	for i, data := range f.chunks {
		chunkDoc := bson.M{
			"_id":      bson.NewObjectId(),
			"files_id": f.id,
			"n":        i,
			"data":     data,
		}
		if _, err := f.gfs.Chunks.mgoColl.InsertOne(ctx, convertMGOToOfficial(chunkDoc)); err != nil {
			return err
		}
	}

	return f.gfs.Chunks.EnsureIndex(Index{
		Key:    []string{"files_id", "n"},
		Unique: true,
	})
}

// Id returns the file ID
func (f *ModernGridFile) Id() interface{} { return f.id }

// SetId sets the file ID
func (f *ModernGridFile) SetId(id interface{}) { f.id = id }

// Name returns the filename
func (f *ModernGridFile) Name() string { return f.filename }

// SetName sets the filename
func (f *ModernGridFile) SetName(filename string) { f.filename = filename }

// ContentType returns the content type
func (f *ModernGridFile) ContentType() string { return f.contentType }

// SetContentType sets the content type
func (f *ModernGridFile) SetContentType(ct string) { f.contentType = ct }

// Size returns the file size
func (f *ModernGridFile) Size() int64 { return f.length }

// MD5 returns the file checksum
func (f *ModernGridFile) MD5() string { return f.md5 }

// UploadDate returns the upload timestamp
func (f *ModernGridFile) UploadDate() time.Time { return f.uploadDate }

// SetUploadDate sets the upload timestamp
func (f *ModernGridFile) SetUploadDate(t time.Time) { f.uploadDate = t }

// GetMeta decodes the metadata into the provided result
func (f *ModernGridFile) GetMeta(result interface{}) error {
	if f.metadata == nil {
		return nil
	}
	return mapStructToInterface(f.metadata, result)
}

// SetMeta sets the metadata object
func (f *ModernGridFile) SetMeta(meta interface{}) { f.metadata = meta }

// SetChunkSize overrides the chunk size used for this file
func (f *ModernGridFile) SetChunkSize(size int) { f.chunkSize = size }
