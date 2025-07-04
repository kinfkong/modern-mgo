// modern_utils.go - Utility functions for modern MongoDB driver compatibility wrapper

package mgo

import (
	stdlog "log"
	"reflect"
	"strings"
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
	case []bson.ObjectId:
		result := make([]interface{}, len(v))
		for i, item := range v {
			if len(item) == 12 {
				objID := primitive.ObjectID{}
				copy(objID[:], []byte(item))
				result[i] = objID
			} else {
				result[i] = item
			}
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
		// Special handling for time.Time conversion from int64 timestamps
		if elementType == reflect.TypeOf(time.Time{}) {
			if timestamp, ok := item.(int64); ok {
				// Convert milliseconds timestamp to time.Time
				timeValue := time.Unix(timestamp/1000, (timestamp%1000)*1000000).UTC()
				newSlice = reflect.Append(newSlice, reflect.ValueOf(timeValue))
				continue
			}
		}

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

	// Handle bson.M conversion to struct - need to preprocess time fields
	if srcMap, ok := src.(bson.M); ok {
		// Get the destination struct type to check field types
		dstValue := reflect.ValueOf(dst)
		if dstValue.Kind() == reflect.Ptr && dstValue.Elem().Kind() == reflect.Struct {
			dstType := dstValue.Elem().Type()
			dstElem := dstValue.Elem()

			// Directly set fields using reflection to avoid bson marshal/unmarshal issues
			for i := 0; i < dstType.NumField(); i++ {
				field := dstType.Field(i)
				fieldValue := dstElem.Field(i)

				// Get the bson field name
				bsonTag := field.Tag.Get("bson")
				tagParts := strings.Split(bsonTag, ",")
				bsonFieldName := tagParts[0]
				if bsonFieldName == "" || bsonFieldName == "-" {
					continue
				}

				// Special handling for _id field
				if bsonFieldName == "_id" && len(tagParts) > 1 && tagParts[1] == "omitempty" {
					bsonFieldName = "_id"
				}

				// Get the value from the map
				mapValue, exists := srcMap[bsonFieldName]
				if !exists {
					continue
				}

				// Handle time slice fields specially
				if field.Type.Kind() == reflect.Slice && field.Type.Elem() == reflect.TypeOf(time.Time{}) {
					// Convert primitive.A to []interface{} if needed
					var slice []interface{}
					switch v := mapValue.(type) {
					case []interface{}:
						slice = v
					case primitive.A:
						slice = []interface{}(v)
					default:
						// Try reflection as last resort
						val := reflect.ValueOf(mapValue)
						if val.Kind() == reflect.Slice {
							slice = make([]interface{}, val.Len())
							for i := 0; i < val.Len(); i++ {
								slice[i] = val.Index(i).Interface()
							}
						}
					}

					if slice != nil {
						timeSlice := reflect.MakeSlice(field.Type, 0, len(slice))
						for _, item := range slice {
							if timestamp, ok := item.(int64); ok {
								timeValue := time.Unix(timestamp/1000, (timestamp%1000)*1000000).UTC()
								timeSlice = reflect.Append(timeSlice, reflect.ValueOf(timeValue))
							} else if t, ok := item.(time.Time); ok {
								timeSlice = reflect.Append(timeSlice, reflect.ValueOf(t))
							} else if dt, ok := item.(primitive.DateTime); ok {
								// Handle primitive.DateTime
								timeValue := dt.Time()
								timeSlice = reflect.Append(timeSlice, reflect.ValueOf(timeValue))
							} else if timestamp, ok := item.(int32); ok {
								// Handle int32 timestamps
								timeValue := time.Unix(int64(timestamp)/1000, (int64(timestamp)%1000)*1000000).UTC()
								timeSlice = reflect.Append(timeSlice, reflect.ValueOf(timeValue))
							} else if timestamp, ok := item.(float64); ok {
								// Handle float64 timestamps (JavaScript numbers)
								ms := int64(timestamp)
								timeValue := time.Unix(ms/1000, (ms%1000)*1000000).UTC()
								timeSlice = reflect.Append(timeSlice, reflect.ValueOf(timeValue))
							}
						}
						if fieldValue.CanSet() {
							fieldValue.Set(timeSlice)
						}
						continue
					}
				}

				// For other fields, use bson unmarshal on individual field
				if fieldValue.CanSet() && fieldValue.CanAddr() {
					fieldData, err := bson.Marshal(bson.M{"temp": mapValue})
					if err == nil {
						var temp bson.M
						if err = bson.Unmarshal(fieldData, &temp); err == nil {
							if tempValue, ok := temp["temp"]; ok {
								// Use reflection to set the value
								setValue := reflect.ValueOf(tempValue)
								if setValue.Type().ConvertibleTo(fieldValue.Type()) {
									fieldValue.Set(setValue.Convert(fieldValue.Type()))
								} else if setValue.Type() == fieldValue.Type() {
									fieldValue.Set(setValue)
								}
							}
						}
					}
				}
			}

			return nil
		}
	}

	// Handle single document conversion for non-struct types
	data, err := bson.Marshal(src)
	if err != nil {
		return err
	}
	return bson.Unmarshal(data, dst)
}

// preprocessTimeSlicesForStruct converts []interface{} containing int64 timestamps to []time.Time
// only if the target struct field is expecting []time.Time
func preprocessTimeSlicesForStruct(value interface{}, fieldName string, structType reflect.Type) interface{} {
	if slice, ok := value.([]interface{}); ok && len(slice) > 0 {
		// Find the field in the struct
		field, found := findStructFieldByBSONTag(structType, fieldName)
		if found && field.Type.Kind() == reflect.Slice && field.Type.Elem() == reflect.TypeOf(time.Time{}) {
			// This field expects []time.Time, so try to convert int64 values
			allInt64 := true
			for _, item := range slice {
				if _, ok := item.(int64); !ok {
					allInt64 = false
					break
				}
			}

			if allInt64 {
				// Convert int64 timestamps to time.Time values
				timeSlice := make([]time.Time, len(slice))
				for i, item := range slice {
					if timestamp, ok := item.(int64); ok {
						timeSlice[i] = time.Unix(timestamp/1000, (timestamp%1000)*1000000).UTC()
					}
				}
				return timeSlice
			}
		}
	}
	return value
}

// findStructFieldByBSONTag finds a struct field by its BSON tag name
func findStructFieldByBSONTag(structType reflect.Type, bsonFieldName string) (reflect.StructField, bool) {
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		bsonTag := field.Tag.Get("bson")

		// Parse the bson tag (format: "fieldname" or "fieldname,omitempty")
		tagParts := strings.Split(bsonTag, ",")
		if len(tagParts) > 0 && tagParts[0] == bsonFieldName {
			return field, true
		}

		// Also check if the field name matches (case-insensitive)
		if strings.ToLower(field.Name) == strings.ToLower(bsonFieldName) {
			return field, true
		}
	}
	return reflect.StructField{}, false
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
