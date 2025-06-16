package mgo

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

type Collection = ModernColl
