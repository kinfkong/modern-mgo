package mgo

import (
	"context"
	"net/url"
	"strings"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Session is an alias of ModernMGO to provide backward compatibility with the
// original mgo API. Existing code that expects a *mgo.Session will continue to
// compile and work without modification.
//
// Example:
//
//	session, err := mgo.Dial("mongodb://localhost:27017/mydb")
//	if err != nil {
//	    ...
//	}
//	defer session.Close()
type Session = ModernMGO

// Dial is a thin wrapper around DialModernMGO that preserves the original mgo
// function signature. It disables retryable writes in the same way that
// DialModernMGO does and returns a *mgo.Session (which is an alias for
// *mgo.ModernMGO).
func Dial(mongoURL string) (*Session, error) {
	return DialModernMGO(mongoURL)
}

// DialWithTimeout replicates the original mgo.DialWithTimeout behaviour using
// the modern MongoDB driver underneath. It establishes a connection to the
// given MongoDB URI but enforces the provided timeout for the initial
// connection handshake.
func DialWithTimeout(mongoURL string, timeout time.Duration) (*Session, error) {
	// Honour zero or negative timeouts by falling back to the default of 10s
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	clientOptions := options.Client().ApplyURI(mongoURL).SetRetryWrites(false)

	client, err := mongodrv.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// Extract default database name from URI path (mirrors legacy behaviour).
	dbName := "test"
	if parsedURL, err := url.Parse(mongoURL); err == nil && parsedURL.Path != "" {
		dbName = strings.TrimPrefix(parsedURL.Path, "/")
		if dbName == "" {
			dbName = "test"
		}
	}

	return &ModernMGO{
		client:     client,
		dbName:     dbName,
		mode:       Primary,
		safe:       &Safe{W: 1},
		isOriginal: true,
	}, nil
}

type Collection = ModernColl
