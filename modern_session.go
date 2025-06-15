// modern_session.go - Session operations for modern MongoDB driver compatibility wrapper

package mgo

import (
	"context"
	"net/url"
	"strings"
	"time"

	officialBson "go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// DialModernMGO connects to MongoDB using the official driver but provides mgo API (mgo API compatible)
func DialModernMGO(mongoURL string) (*ModernMGO, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Disable retryable writes to avoid "Retryable writes are not supported" error
	clientOptions := options.Client().ApplyURI(mongoURL).SetRetryWrites(false)

	client, err := mongodrv.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// Parse database name from URL
	dbName := "test" // Default database name
	if parsedURL, err := url.Parse(mongoURL); err == nil && parsedURL.Path != "" {
		dbName = strings.TrimPrefix(parsedURL.Path, "/")
		if dbName == "" {
			dbName = "test"
		}
	}

	return &ModernMGO{
		client: client,
		dbName: dbName,
		mode:   Primary,
		safe: &Safe{
			W:        1,
			WTimeout: 0,
			FSync:    false,
			J:        false,
		},
		isOriginal: true, // Mark as original session
	}, nil
}

// Close closes the modern MGO session
func (m *ModernMGO) Close() {
	// Only close the client if this is the original session
	if m.isOriginal && m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		m.client.Disconnect(ctx)
	}
}

// Copy creates a copy of the session (mgo API compatible)
func (m *ModernMGO) Copy() *ModernMGO {
	return &ModernMGO{
		client:     m.client, // Reuse the same client connection
		dbName:     m.dbName,
		mode:       m.mode,
		safe:       m.safe,
		isOriginal: false, // Mark as copy
	}
}

// Clone creates a clone of the session (mgo API compatible)
func (m *ModernMGO) Clone() *ModernMGO {
	return m.Copy() // In our implementation, Clone behaves like Copy
}

// SetMode sets the session mode for read preference (mgo API compatible)
func (m *ModernMGO) SetMode(mode Mode, refresh bool) {
	m.mode = mode
	// Note: refresh parameter is for mgo compatibility but not used in modern driver
}

// Mode returns the current session mode
func (m *ModernMGO) Mode() Mode {
	return m.mode
}

// getReadPreference converts mgo Mode to official driver ReadPreference
func (m *ModernMGO) getReadPreference() *readpref.ReadPref {
	switch m.mode {
	case Primary:
		return readpref.Primary()
	case PrimaryPreferred:
		return readpref.PrimaryPreferred()
	case Secondary:
		return readpref.Secondary()
	case SecondaryPreferred:
		return readpref.SecondaryPreferred()
	case Nearest:
		return readpref.Nearest()
	default:
		return readpref.Primary()
	}
}

// Ping tests the connection
func (m *ModernMGO) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return m.client.Ping(ctx, readpref.Primary())
}

// BuildInfo gets server build information (mgo API compatible)
func (m *ModernMGO) BuildInfo() (BuildInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := m.client.Database("admin")

	var result struct {
		Version        string `bson:"version"`
		GitVersion     string `bson:"gitVersion"`
		SysInfo        string `bson:"sysInfo"`
		Bits           int    `bson:"bits"`
		Debug          bool   `bson:"debug"`
		MaxObjectSize  int    `bson:"maxBsonObjectSize"`
		VersionArray   []int  `bson:"versionArray"`
		OpenSSLVersion string `bson:"OpenSSLVersion"`
	}

	err := db.RunCommand(ctx, officialBson.M{"buildInfo": 1}).Decode(&result)
	if err != nil {
		return BuildInfo{}, err
	}

	return BuildInfo{
		Version:        result.Version,
		GitVersion:     result.GitVersion,
		SysInfo:        result.SysInfo,
		Bits:           result.Bits,
		Debug:          result.Debug,
		MaxObjectSize:  result.MaxObjectSize,
		VersionArray:   result.VersionArray,
		OpenSSLVersion: result.OpenSSLVersion,
	}, nil
}

// DB returns a database handle
func (m *ModernMGO) DB(name string) *ModernDB {
	if name == "" {
		name = m.dbName
	}
	return &ModernDB{
		mgoDB: m.client.Database(name),
		name:  name,
	}
}

// C returns a collection handle
func (db *ModernDB) C(name string) *ModernColl {
	return &ModernColl{
		mgoColl: db.mgoDB.Collection(name),
		name:    name,
	}
}

// GridFS returns a GridFS handle (mgo API compatible)
func (db *ModernDB) GridFS(prefix string) *ModernGridFS {
	return &ModernGridFS{
		Files:  db.C(prefix + ".files"),
		Chunks: db.C(prefix + ".chunks"),
		prefix: prefix,
	}
}

// Run executes a database command (mgo API compatible)
func (db *ModernDB) Run(cmd interface{}, result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	command := convertMGOToOfficial(cmd)
	return db.mgoDB.RunCommand(ctx, command).Decode(result)
}

// Run executes a database command (mgo API compatible with 3-parameter interface)
func (m *ModernMGO) Run(adminFlag interface{}, cmd interface{}, result interface{}) error {
	// First parameter determines which database to use
	// If true or admin-like, use admin database; otherwise use default database
	var dbName string

	switch v := adminFlag.(type) {
	case bool:
		if v {
			dbName = "admin"
		} else {
			dbName = m.dbName
		}
	case string:
		if v == "admin" || v == "true" {
			dbName = "admin"
		} else {
			dbName = m.dbName
		}
	default:
		// Default to admin for backward compatibility
		dbName = "admin"
	}

	return m.DB(dbName).Run(cmd, result)
}
