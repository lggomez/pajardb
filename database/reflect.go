package database

import (
	//"errors"
	"fmt"
	"reflect"
	"strings"
)

var tokenCache = map[string][]string{}

func tokenizeField(fieldName string) []string {
	var fieldTokens []string
	if tokens, found := tokenCache[fieldName]; found {
		fieldTokens = tokens
	} else {
		fieldTokens = strings.Split(fieldName, ".")
		tokenCache[fieldName] = fieldTokens
	}
	return fieldTokens
}

func findStructValueFromFieldName(fieldName string, value PtrValue) (StructValue, error) {
	var pivot reflect.Value
	pivot = reflect.ValueOf(value)

	// Traverse object to the correct field level
	for _, field := range tokenizeField(fieldName) {
		pivot = reflect.Indirect(pivot)
		pivot = pivot.FieldByName(field)
		if !pivot.IsValid() {
			return nil, fmt.Errorf("error getting field '%s' from value %v", fieldName, value)
		}
	}

	return pivot.Interface(), nil
}

func findFieldOnType(fieldName string, entityType reflect.Type) (reflect.StructField, bool) {
	currentType := entityType
	var currentField reflect.StructField
	var found bool
	// Traverse type to the correct field level
	for _, field := range tokenizeField(fieldName) {
		currentField, found = currentType.FieldByName(field)
		if !found {
			return currentField, false
		}
		currentType = currentField.Type
	}
	return currentField, true
}

func validateKeyKind(kind reflect.Kind) error {
	switch kind {
	case reflect.Invalid:
		fallthrough
	case reflect.Bool:
		fallthrough
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.Complex64:
		fallthrough
	case reflect.Complex128:
		fallthrough
	case reflect.Array:
		fallthrough
	case reflect.Chan:
		fallthrough
	case reflect.Func:
		fallthrough
	case reflect.Interface:
		fallthrough
	case reflect.Map:
		fallthrough
	case reflect.Ptr:
		fallthrough
	case reflect.Slice:
		fallthrough
	case reflect.Struct:
		fallthrough
	case reflect.UnsafePointer:
		return fmt.Errorf("must be a string or integer kind, got '%v'", kind)
	}
	return nil
}
