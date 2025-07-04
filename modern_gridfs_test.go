package mgo_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/globalsign/mgo/bson"
)

func TestModernGridFSCreate(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	gfs := tdb.DB().GridFS("fs")

	// Create a new file
	file, err := gfs.Create("test.txt")
	AssertNoError(t, err, "Failed to create GridFS file")

	// Set metadata
	file.SetContentType("text/plain")
	file.SetMeta(bson.M{"author": "test", "version": 1})

	// Write data
	data := []byte("Hello, GridFS!")
	n, err := file.Write(data)
	AssertNoError(t, err, "Failed to write to GridFS file")
	AssertEqual(t, len(data), n, "Incorrect number of bytes written")

	// Close the file
	err = file.Close()
	AssertNoError(t, err, "Failed to close GridFS file")

	// Verify file properties after close
	id := file.Id()
	if id == nil {
		t.Fatal("File ID is nil")
	}

	if file.Name() != "test.txt" {
		t.Fatalf("Expected filename 'test.txt', got '%s'", file.Name())
	}

	if file.Size() != int64(len(data)) {
		t.Fatalf("Expected size %d, got %d", len(data), file.Size())
	}
}

func TestModernGridFSOpenAndRead(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	gfs := tdb.DB().GridFS("fs")

	// Create and write a file
	file, err := gfs.Create("read_test.txt")
	AssertNoError(t, err, "Failed to create GridFS file")

	testData := []byte("This is test data for reading")
	_, err = file.Write(testData)
	AssertNoError(t, err, "Failed to write test data")

	err = file.Close()
	AssertNoError(t, err, "Failed to close file after writing")

	// Open the file for reading
	file, err = gfs.Open("read_test.txt")
	AssertNoError(t, err, "Failed to open GridFS file")
	defer file.Close()

	// Read the data
	buffer := make([]byte, len(testData))
	n, err := file.Read(buffer)
	AssertNoError(t, err, "Failed to read from GridFS file")
	AssertEqual(t, len(testData), n, "Incorrect number of bytes read")

	// Verify data
	if !bytes.Equal(testData, buffer) {
		t.Fatal("Read data does not match written data")
	}
}

func TestModernGridFSOpenId(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	gfs := tdb.DB().GridFS("fs")

	// Create a file
	file, err := gfs.Create("file_with_id.txt")
	AssertNoError(t, err, "Failed to create GridFS file")

	_, err = file.Write([]byte("Data for ID test"))
	AssertNoError(t, err, "Failed to write data")

	err = file.Close()
	AssertNoError(t, err, "Failed to close file")

	fileId := file.Id()

	// Open by ID
	file2, err := gfs.OpenId(fileId)
	AssertNoError(t, err, "Failed to open file by ID")
	defer file2.Close()

	// Verify it's the same file
	if file2.Name() != "file_with_id.txt" {
		t.Fatalf("Expected filename 'file_with_id.txt', got '%s'", file2.Name())
	}
}

// Note: Seek is not implemented in the modern wrapper

func TestModernGridFSRemove(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	gfs := tdb.DB().GridFS("fs")

	// Create a file
	file, err := gfs.Create("to_remove.txt")
	AssertNoError(t, err, "Failed to create GridFS file")

	_, err = file.Write([]byte("This file will be removed"))
	AssertNoError(t, err, "Failed to write data")

	err = file.Close()
	AssertNoError(t, err, "Failed to close file")

	// Remove the file
	err = gfs.Remove("to_remove.txt")
	AssertNoError(t, err, "Failed to remove GridFS file")

	// Verify file is gone
	_, err = gfs.Open("to_remove.txt")
	AssertError(t, err, "Expected error when opening removed file")
}

func TestModernGridFSRemoveId(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	gfs := tdb.DB().GridFS("fs")

	// Create a file
	file, err := gfs.Create("to_remove_by_id.txt")
	AssertNoError(t, err, "Failed to create GridFS file")

	_, err = file.Write([]byte("This file will be removed by ID"))
	AssertNoError(t, err, "Failed to write data")

	err = file.Close()
	AssertNoError(t, err, "Failed to close file")

	fileId := file.Id()

	// Remove by ID
	err = gfs.RemoveId(fileId)
	AssertNoError(t, err, "Failed to remove GridFS file by ID")

	// Verify file is gone
	_, err = gfs.OpenId(fileId)
	AssertError(t, err, "Expected error when opening removed file")
}

func TestModernGridFSLargeFile(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	gfs := tdb.DB().GridFS("fs")

	// Create a large file (multiple chunks)
	file, err := gfs.Create("large_file.bin")
	AssertNoError(t, err, "Failed to create GridFS file")

	// Generate large data (1MB)
	chunkSize := 256 * 1024         // Default GridFS chunk size
	totalSize := chunkSize*4 + 1234 // Multiple chunks plus partial
	largeData := make([]byte, totalSize)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Write in pieces
	written := 0
	pieceSize := 100000
	for written < totalSize {
		end := written + pieceSize
		if end > totalSize {
			end = totalSize
		}
		n, err := file.Write(largeData[written:end])
		AssertNoError(t, err, "Failed to write chunk")
		written += n
	}

	err = file.Close()
	AssertNoError(t, err, "Failed to close large file")

	// Verify size
	if file.Size() != int64(totalSize) {
		t.Fatalf("Expected size %d, got %d", totalSize, file.Size())
	}

	// Read back and verify
	file, err = gfs.Open("large_file.bin")
	AssertNoError(t, err, "Failed to open large file")
	defer file.Close()

	readData := make([]byte, totalSize)
	totalRead := 0
	for totalRead < totalSize {
		n, err := file.Read(readData[totalRead:])
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read: %v", err)
		}
		totalRead += n
		if err == io.EOF {
			break
		}
	}

	AssertEqual(t, totalSize, totalRead, "Incorrect number of bytes read")

	// Verify data integrity
	if !bytes.Equal(largeData, readData) {
		t.Fatal("Read data does not match written data")
	}
}

func TestModernGridFSMetadata(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	gfs := tdb.DB().GridFS("fs")

	// Create file with metadata
	file, err := gfs.Create("metadata_test.txt")
	AssertNoError(t, err, "Failed to create GridFS file")

	metadata := bson.M{
		"author":  "John Doe",
		"version": 2,
		"tags":    []string{"test", "gridfs", "metadata"},
		"properties": bson.M{
			"encoding": "utf-8",
			"language": "en",
		},
	}

	file.SetMeta(metadata)
	file.SetContentType("text/plain; charset=utf-8")

	_, err = file.Write([]byte("File with metadata"))
	AssertNoError(t, err, "Failed to write data")

	err = file.Close()
	AssertNoError(t, err, "Failed to close file")

	// Read back and verify metadata
	file, err = gfs.Open("metadata_test.txt")
	AssertNoError(t, err, "Failed to open file")
	defer file.Close()

	// Check content type
	if file.ContentType() != "text/plain; charset=utf-8" {
		t.Fatalf("Expected content type 'text/plain; charset=utf-8', got '%s'", file.ContentType())
	}

	// Check metadata
	var metaResult bson.M
	err = file.GetMeta(&metaResult)
	AssertNoError(t, err, "Failed to get metadata")

	if metaResult["author"] != "John Doe" {
		t.Fatalf("Expected author 'John Doe', got '%v'", metaResult["author"])
	}
}

func TestModernGridFSMultipleFiles(t *testing.T) {
	// Setup
	tdb := NewTestDB(t)
	defer tdb.Close(t)

	gfs := tdb.DB().GridFS("fs")

	// Create multiple files with same name (versions)
	for i := 1; i <= 3; i++ {
		file, err := gfs.Create("versioned.txt")
		AssertNoError(t, err, "Failed to create GridFS file")

		file.SetMeta(bson.M{"version": i})

		data := []byte("Version " + string(rune('0'+i)))
		_, err = file.Write(data)
		AssertNoError(t, err, "Failed to write data")

		err = file.Close()
		AssertNoError(t, err, "Failed to close file")
	}

	// Open should get the latest version
	file, err := gfs.Open("versioned.txt")
	AssertNoError(t, err, "Failed to open file")
	defer file.Close()

	// Read and verify it's the latest version
	data := make([]byte, 10)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read: %v", err)
	}

	if string(data[:n]) != "Version 3" {
		t.Fatalf("Expected 'Version 3', got '%s'", string(data[:n]))
	}
}
