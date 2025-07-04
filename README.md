# modern-mgo

A modern MongoDB driver wrapper that provides `mgo`-compatible API using the official MongoDB Go driver.

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Testing](#testing)
  - [Prerequisites](#prerequisites)
  - [Running Tests](#running-tests)
  - [Test Infrastructure](#test-infrastructure)
  - [Test Coverage](#test-coverage)
- [Usage](#usage)
- [API Compatibility](#api-compatibility)

## Overview

This project provides a compatibility layer that allows applications using the legacy `mgo` (globalsign/mgo) driver to work with the modern official MongoDB Go driver. It implements the familiar mgo API while using the official driver under the hood.

## Installation

```bash
go get github.com/globalsign/mgo
```

## Testing

### Prerequisites

To run the tests, you need:

1. **Docker** and **Docker Compose** installed on your system
2. **Go** 1.16 or higher
3. **Make** (usually pre-installed on Unix systems)

### Running Tests

The project uses a Docker-based test infrastructure with MongoDB running in a container. Here are the main commands:

#### Quick Start

```bash
# Run all tests (automatically sets up and tears down the test database)
make test

# Run only unit tests
make test-unit

# Run integration tests
make test-integration
```

#### Test Database Management

```bash
# Manually start the test MongoDB instance (port 27018)
make setup-test-db

# Stop and remove the test MongoDB instance
make teardown-test-db

# Start MongoDB Express UI for database inspection (http://localhost:8081)
make mongo-express
```

#### Advanced Testing Options

```bash
# Run tests with coverage report
make test-coverage
# Coverage report will be generated as coverage.html

# Run tests with race detection
make test-race

# Run tests with verbose output
make test-verbose

# Run a specific test
make test-specific TEST=TestModernSessionPing

# Run benchmarks
make benchmark

# Clean up test artifacts and Docker volumes
make clean
```

### Test Infrastructure

The test setup includes:

- **MongoDB 4.4** running in Docker on port 27018
- **Isolated test database** with automatic cleanup
- **Test utilities** for common operations and assertions
- **Sample test data** for consistent testing

#### Directory Structure

```
modern-mgo/
├── test/
│   ├── docker-compose.test.yml  # Docker setup for test MongoDB
│   └── Makefile                 # Test-specific make targets
├── Makefile                     # Root makefile (delegates to test/)
├── test_utils_test.go          # Common test utilities
└── modern_*_test.go            # Test files for each module
```

#### Test Files

- `modern_session_test.go` - Session management and database operations
- `modern_collection_test.go` - CRUD operations, bulk operations, indexes, complex data types
- `modern_query_test.go` - Query operations, sorting, filtering, complex queries
- `modern_iterator_test.go` - Cursor iteration and result handling
- `modern_aggregation_test.go` - Aggregation pipeline operations
- `modern_bulk_test.go` - Bulk write operations
- `modern_gridfs_test.go` - GridFS file storage operations
- `bson_objectid_test.go` - BSON ObjectId operations and conversions

### Test Coverage

The test suite covers the main functionality of the mgo-compatible API:

✅ **Implemented and Tested:**
- Session management (Dial, Copy, Clone, Close)
- Database operations
- Collection CRUD operations (Insert, Find, Update, Remove)
- Query operations (Sort, Limit, Skip, Select, Count)
- Complex queries ($or, $and, $not, $ne, $in, $all, $elemMatch)
- Time-based filtering ($gte, $lt, $lte, $eq with dates)
- Pagination with Skip and Limit
- Aggregation pipelines
- Bulk operations
- GridFS file operations
- Index management
- BSON ObjectId operations (ObjectIdHex, IsObjectIdHex)
- Complex data types (time slices, nested structs, map[string]interface{})
- Nil handling for pointers and slices

❌ **Not Implemented** (from original mgo):
- Session: `SetSyncTimeout`, `Refresh`, `DatabaseNames`, `FindRef`, `SetSafe`
- Query: `Explain`, `Hint`, `Batch`, `SetMaxTime`
- Iterator: `Err`, `Timeout`
- Collection: `Distinct`, `DropIndex`, `Create` with CollectionInfo
- GridFS: `Seek` method

## Usage

Basic example of using the modern-mgo wrapper:

```go
package main

import (
    "log"
    "github.com/globalsign/mgo"
    "github.com/globalsign/mgo/bson"
)

func main() {
    // Connect to MongoDB
    session, err := mgo.DialModernMGO("mongodb://localhost:27017/mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer session.Close()

    // Get collection
    c := session.DB("mydb").C("mycollection")

    // Insert document
    err = c.Insert(bson.M{"name": "John", "age": 30})
    if err != nil {
        log.Fatal(err)
    }

    // Find document
    var result bson.M
    err = c.Find(bson.M{"name": "John"}).One(&result)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Found: %v", result)
}
```

## API Compatibility

This wrapper aims to provide drop-in compatibility for applications using `mgo`. Most common operations are supported, allowing for gradual migration to the official MongoDB driver.

For detailed API documentation, refer to the original [mgo documentation](https://godoc.org/github.com/globalsign/mgo), keeping in mind the limitations listed in the test coverage section above.

## Contributing

When adding new features or fixing bugs:

1. Write tests for your changes
2. Ensure all tests pass: `make test`
3. Check race conditions: `make test-race`
4. Update documentation as needed

## License

This project maintains compatibility with the original mgo license. See LICENSE file for details. 