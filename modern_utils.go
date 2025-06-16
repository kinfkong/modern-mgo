// modern_utils.go - Utility functions for modern MongoDB driver compatibility wrapper

package mgo

import (
	stdlog "log"
	"reflect"
	"time"

	"github.com/globalsign/mgo/bson"
	officialBson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Debug flag to enable conversion debugging
var DebugConversion = false

// Conversion helpers
func convertMGOToOfficial(input interface{}) interface{} {
	if input == nil {
		return nil
	}

	// Handle pointers by dereferencing them
	val := reflect.ValueOf(input)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		return convertMGOToOfficial(val.Elem().Interface())
	}

	switch v := input.(type) {
	case bson.M:
		result := officialBson.M{}
		for key, value := range v {
			result[key] = convertMGOToOfficial(value)
		}
		return result
	case bson.D:
		// Convert bson.D to officialBson.D to preserve order (important for commands)
		result := officialBson.D{}
		for _, elem := range v {
			result = append(result, officialBson.E{
				Key:   elem.Name,
				Value: convertMGOToOfficial(elem.Value),
			})
		}
		return result
	case []bson.M:
		// Handle []bson.M specifically for $or, $and, etc. query operators
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = convertMGOToOfficial(item)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = convertMGOToOfficial(item)
		}
		return result
	case map[string]interface{}:
		result := officialBson.M{}
		for key, value := range v {
			result[key] = convertMGOToOfficial(value)
		}
		return result
	case bson.ObjectId:
		if len(v) == 12 {
			objID := primitive.ObjectID{}
			copy(objID[:], []byte(v))
			return objID
		}
		return v
	case time.Time:
		// Convert time.Time to primitive.DateTime
		return primitive.NewDateTimeFromTime(v)
	default:
		// Check if it's a slice of bson.M using reflection
		if val.Kind() == reflect.Slice {
			elemType := val.Type().Elem()
			if elemType == reflect.TypeOf(bson.M{}) {
				// Handle slice of bson.M
				result := make([]interface{}, val.Len())
				for i := 0; i < val.Len(); i++ {
					result[i] = convertMGOToOfficial(val.Index(i).Interface())
				}
				return result
			}
		}

		// Handle structs by marshaling/unmarshaling with bson tags
		if val.Kind() == reflect.Struct || (val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Struct) {
			// Marshal to bson, then unmarshal to map to respect bson tags
			data, err := bson.Marshal(input)
			if err != nil {
				return input // fallback to original
			}
			var result bson.M
			err = bson.Unmarshal(data, &result)
			if err != nil {
				return input // fallback to original
			}
			return convertMGOToOfficial(result)
		}
		return v
	}
}

func convertOfficialToMGO(input interface{}) interface{} {
	if input == nil {
		return nil
	}

	switch v := input.(type) {
	case officialBson.M:
		result := bson.M{}
		for key, value := range v {
			result[key] = convertOfficialToMGO(value)
		}
		return result
	case officialBson.D:
		result := bson.D{}
		for _, elem := range v {
			result = append(result, bson.DocElem{
				Name:  elem.Key,
				Value: convertOfficialToMGO(elem.Value),
			})
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = convertOfficialToMGO(item)
		}
		return result
	case map[string]interface{}:
		result := bson.M{}
		for key, value := range v {
			result[key] = convertOfficialToMGO(value)
		}
		return result
	case primitive.ObjectID:
		return bson.ObjectId(v[:])
	case primitive.DateTime:
		// Convert primitive.DateTime to time.Time
		return v.Time()
	default:
		return v
	}
}

// convertSliceWithReflect converts a slice of interfaces to a target slice type using reflection
func convertSliceWithReflect(srcSlice []interface{}, dst interface{}) error {
	dstValue := reflect.ValueOf(dst)
	if dstValue.Kind() != reflect.Ptr {
		return ErrNotFound
	}

	dstSlice := dstValue.Elem()
	if dstSlice.Kind() != reflect.Slice {
		return ErrNotFound
	}

	elementType := dstSlice.Type().Elem()
	newSlice := reflect.MakeSlice(dstSlice.Type(), 0, len(srcSlice))

	for _, item := range srcSlice {
		// Convert each item to the target element type
		newElement := reflect.New(elementType).Interface()
		err := mapStructToInterface(item, newElement)
		if err != nil {
			return err
		}
		newSlice = reflect.Append(newSlice, reflect.ValueOf(newElement).Elem())
	}

	dstSlice.Set(newSlice)
	return nil
}

func mapStructToInterface(src, dst interface{}) error {
	if src == nil {
		return ErrNotFound
	}

	// Handle slice conversion specifically
	if srcSlice, ok := src.([]interface{}); ok {
		// Use reflection to handle slice conversion properly
		return convertSliceWithReflect(srcSlice, dst)
	}

	// Handle single document conversion
	data, err := bson.Marshal(src)
	if err != nil {
		return err
	}
	return bson.Unmarshal(data, dst)
}

// ensureObjectId ensures that a document has a proper _id field
func ensureObjectId(doc interface{}) interface{} {
	if doc == nil {
		return doc
	}

	switch v := doc.(type) {
	case bson.M:
		if _, hasId := v["_id"]; !hasId {
			v["_id"] = bson.NewObjectId()
		}
		return v
	case map[string]interface{}:
		if _, hasId := v["_id"]; !hasId {
			v["_id"] = bson.NewObjectId()
		}
		return v
	default:
		// For struct types, use reflection to check for _id field
		val := reflect.ValueOf(doc)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		if val.Kind() == reflect.Struct {
			// Try to find an _id field or Id field
			idField := val.FieldByName("Id")
			if !idField.IsValid() {
				idField = val.FieldByName("ID")
			}
			if !idField.IsValid() {
				// Look for bson:"_id" tag
				for i := 0; i < val.NumField(); i++ {
					field := val.Type().Field(i)
					if tag := field.Tag.Get("bson"); tag == "_id" || tag == "_id,omitempty" {
						idField = val.Field(i)
						break
					}
				}
			}

			if idField.IsValid() && idField.CanSet() {
				// Check if the field is zero/empty
				if idField.Kind() == reflect.String && idField.String() == "" {
					idField.SetString(string(bson.NewObjectId()))
				} else if idField.Type() == reflect.TypeOf(bson.ObjectId("")) {
					if idField.String() == "" {
						idField.Set(reflect.ValueOf(bson.NewObjectId()))
					}
				}
			}
		}
		return doc
	}
}

// convertMGOToOfficialWithDebug is a debug version that logs conversions
func convertMGOToOfficialWithDebug(input interface{}, depth int) interface{} {
	indent := ""
	for i := 0; i < depth; i++ {
		indent += "  "
	}

	if DebugConversion {
		stdlog.Printf("%sConverting: %T = %v", indent, input, input)
	}

	result := convertMGOToOfficial(input)

	if DebugConversion {
		stdlog.Printf("%sResult: %T = %v", indent, result, result)
	}

	return result
}

// ConvertMGOToOfficialDebug is a public debug function
func ConvertMGOToOfficialDebug(input interface{}) interface{} {
	DebugConversion = true
	defer func() { DebugConversion = false }()
	return convertMGOToOfficialWithDebug(input, 0)
}
