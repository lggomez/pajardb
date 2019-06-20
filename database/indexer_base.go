package database

import (
	"errors"
	"fmt"
	"reflect"
)

type InverseIndex map[StructValue][]PtrValue

type BaseIndexer struct {
	index     InverseIndex
	fieldName string
}

func newBaseIndexer(fieldName string, entityType reflect.Type) (*BaseIndexer, error) {
	index := InverseIndex{}
	b := &BaseIndexer{index, fieldName}

	if fieldName == "" {
		return b, errors.New("field name is required")
	}

	if err := b.validate(entityType); err != nil {
		return b, fmt.Errorf("invalid indexer %s: %s", fieldName, err.Error())
	}

	return b, nil
}

func (b *BaseIndexer) insert(value PtrValue) error {
	key, keyErr := findStructValueFromFieldName(b.fieldName, value)

	if keyErr != nil {
		return keyErr
	}

	if _, ok := b.index[key]; !ok {
		b.index[key] = []PtrValue{value}
	} else {
		b.index[key] = append(b.index[key], value)
	}

	return nil
}

func (b *BaseIndexer) validate(entityType reflect.Type) error {
	if _, found := findFieldOnType(b.fieldName, entityType); !found {
		return fmt.Errorf("field %s not found on for type %s", b.fieldName, entityType.String())
	}
	return nil
}

func (b *BaseIndexer) Field() string {
	return b.fieldName
}

func (b *BaseIndexer) Size() int {
	return len(b.index)
}
