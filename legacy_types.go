package mgo

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/globalsign/mgo/bson"
)

// Mode specifies the replica-set read preference mode (compatibility with mgo).
// The numeric values match those in the original mgo driver for drop-in parity.
//
// Relevant MongoDB documentation:
//
//	https://docs.mongodb.com/manual/reference/read-preference/
//
// NOTE: Only the constants are required by the wrapper – the semantics are
// handled inside the modern implementation via getReadPreference().
type Mode int

const (
	// Primary mode – all operations read from the primary.
	Primary Mode = 2
	// PrimaryPreferred – read from primary if available otherwise secondary.
	PrimaryPreferred Mode = 3
	// Secondary – read from one of the secondaries only.
	Secondary Mode = 4
	// SecondaryPreferred – read from secondary if available otherwise primary.
	SecondaryPreferred Mode = 5
	// Nearest – read from the node with lowest network latency regardless of role.
	Nearest Mode = 6

	// Eventual, Monotonic and Strong are mgo-specific legacy aliases kept for
	// API compatibility. They map internally onto the standard modes above.
	Eventual  Mode = 0
	Monotonic Mode = 1
	Strong    Mode = 2
)

// Safe replicates the mgo Safe struct so that callers can specify write concern
// options in an API-compatible way. Only the fields referenced by the modern
// wrapper are included.
type Safe struct {
	W        int    // Minimum number of servers to acknowledge the write
	WMode    string // Write mode string, e.g. "majority"
	RMode    string // Read mode string used with newer MongoDB versions
	WTimeout int    // Timeout (ms) for write acknowledgement
	FSync    bool   // Force fsync (legacy)
	J        bool   // Wait for the journal commit
}

// ErrNotFound is returned when a requested document is not present. Many
// higher-level helper methods rely on comparing against this sentinel value.
var ErrNotFound = errors.New("not found")

// -------------------------- Index & Collation --------------------------

// Index mirrors the original mgo Index definition but only exposes the fields
// required by the modern compatibility layer.
type Index struct {
	Key           []string // Index key specification ("field" or "-field" for desc)
	Unique        bool     // Enforce uniqueness
	DropDups      bool     // Drop duplicates when creating a unique index (legacy)
	Background    bool     // Build index in the background
	Sparse        bool     // Only index documents containing the key
	PartialFilter bson.M   // Partial index filter expression

	// TTL index: documents older than ExpireAfter will be automatically removed.
	ExpireAfter time.Duration

	// Name explicitly sets the index name; if empty the server auto-generates it.
	Name string

	// Geo / text specific options (kept for completeness – unused by wrapper).
	Min, Max   int
	Minf, Maxf float64
	BucketSize float64
	Bits       int

	DefaultLanguage  string
	LanguageOverride string

	// Field weights for text indexes.
	Weights map[string]int

	// Collation to use for string comparison rules.
	Collation *Collation
}

// Collation specifies language-specific rules for string comparison.
// It matches the structure used by MongoDB 3.4+.
type Collation struct {
	Locale          string `bson:"locale"`
	CaseFirst       string `bson:"caseFirst,omitempty"`
	Strength        int    `bson:"strength,omitempty"`
	Alternate       string `bson:"alternate,omitempty"`
	MaxVariable     string `bson:"maxVariable,omitempty"`
	Normalization   bool   `bson:"normalization,omitempty"`
	CaseLevel       bool   `bson:"caseLevel,omitempty"`
	NumericOrdering bool   `bson:"numericOrdering,omitempty"`
	Backwards       bool   `bson:"backwards,omitempty"`
}

// --------------------------- ChangeInfo ---------------------------

// ChangeInfo captures the outcome of update/delete operations returning exact
// document counts in a way that mirrors the original driver.
type ChangeInfo struct {
	Updated    int         // Number of existing documents modified
	Removed    int         // Number of documents removed
	Matched    int         // Number of documents matched (may differ from Updated)
	UpsertedId interface{} // _id of an upserted document when not explicitly set
}

// ----------------------- Bulk operation results -----------------------

type BulkResult struct {
	Matched  int // Number of documents matched by the operation
	Modified int // Number of documents actually modified (MongoDB 2.6+ only)

	// Additional fields present in the original implementation are omitted
	// as the modern wrapper does not rely on them. The struct layout is kept
	// compatible so client code can embed it without changes.
	private bool
}

// BulkErrorCase stores the error and the index (position) within a bulk
// operation that generated it.
type BulkErrorCase struct {
	Index int   // Position of the failed operation (-1 if unknown)
	Err   error // The underlying error
}

// BulkError aggregates one or more BulkErrorCase instances.
type BulkError struct {
	ecases []BulkErrorCase
}

// Error implements the standard error interface, producing a human-readable
// summary of one or multiple bulk errors.
func (e *BulkError) Error() string {
	if len(e.ecases) == 0 {
		return "invalid BulkError instance: no errors"
	}
	if len(e.ecases) == 1 {
		return e.ecases[0].Err.Error()
	}
	var buf bytes.Buffer
	buf.WriteString("multiple errors in bulk operation:\n")
	seen := make(map[string]bool, len(e.ecases))
	for _, c := range e.ecases {
		msg := c.Err.Error()
		if !seen[msg] {
			seen[msg] = true
			buf.WriteString("  - ")
			buf.WriteString(msg)
			buf.WriteByte('\n')
		}
	}
	return buf.String()
}

// Cases exposes the individual error cases contained in the BulkError.
func (e *BulkError) Cases() []BulkErrorCase {
	return e.ecases
}

// --------------------------- BuildInfo ---------------------------

// BuildInfo holds server build details returned by the buildInfo command.
type BuildInfo struct {
	Version        string
	VersionArray   []int  `bson:"versionArray"`
	GitVersion     string `bson:"gitVersion"`
	OpenSSLVersion string `bson:"OpenSSLVersion"`
	SysInfo        string `bson:"sysInfo"`
	Bits           int
	Debug          bool
	MaxObjectSize  int `bson:"maxBsonObjectSize"`
}

// VersionAtLeast reports whether the server version is greater than or equal
// to the supplied version tuple.
func (bi *BuildInfo) VersionAtLeast(version ...int) bool {
	for i, v := range version {
		if i >= len(bi.VersionArray) {
			return false
		}
		if bi.VersionArray[i] > v {
			return true
		}
		if bi.VersionArray[i] < v {
			return false
		}
	}
	return true
}

// --------------------------- Change struct ---------------------------

// Change represents the set of possible modifications applied by Query.Apply.
type Change struct {
	Update    interface{} // Document describing the modification to apply
	Upsert    bool        // Insert the document if it doesn't exist
	Remove    bool        // Remove the matched document instead of updating
	ReturnNew bool        // Return the modified rather than the original doc
}

// -------------------------- QueryError --------------------------

// QueryError mirrors mgo.QueryError, providing code & message.
type QueryError struct {
	Code      int
	Message   string
	Assertion bool
}

func (err *QueryError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.Code != 0 {
		return err.Message + " (code " + strconv.Itoa(err.Code) + ")"
	}
	return err.Message
}

// ---------------------- update helpers ----------------------

// hasUpdateOperators returns true if the provided document already contains a
// top-level MongoDB update operator (keys starting with "$").
func hasUpdateOperators(doc interface{}) bool {
	if doc == nil {
		return false
	}
	switch d := doc.(type) {
	case bson.M:
		for k := range d {
			if strings.HasPrefix(k, "$") {
				return true
			}
		}
	case map[string]interface{}:
		for k := range d {
			if strings.HasPrefix(k, "$") {
				return true
			}
		}
	}
	return false
}

// wrapInSetOperator ensures plain replacement documents are converted into a
// $set update so they behave consistently across drivers.
func wrapInSetOperator(doc interface{}) interface{} {
	if hasUpdateOperators(doc) {
		return doc
	}
	return bson.M{"$set": doc}
}
