package database

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type Table struct {
	elements []PtrValue
	schema   TableSchema
	lock     *sync.RWMutex
}

type TableSchema struct {
	name       string
	entityType reflect.Type
	indexers   map[string]*BaseIndexer
}

func (t *Table) insert(element reflect.Value) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	e := element.Interface()

	t.elements = append(t.elements, e)

	for _, indexer := range t.schema.indexers {
		if insertErr := indexer.insert(e); insertErr != nil {
			return fmt.Errorf("index insert error: %s", insertErr.Error())
		}
	}

	return nil
}

func NewTableSchema(name string, entity StructValue) (TableSchema, error) {
	entityType := reflect.TypeOf(entity)

	if entityType.Kind() == reflect.Ptr {
		return TableSchema{}, errors.New("pointers are not valid entities")
	}

	indexers := map[string]*BaseIndexer{}
	return TableSchema{name, entityType, indexers}, nil
}

func (t *TableSchema) AddIndex(fieldName string) error {
	indexer, indexerErr := newBaseIndexer(fieldName, t.entityType)
	if indexerErr != nil {
		return fmt.Errorf("error creating indexer for field %s: %s", fieldName, indexerErr.Error())
	}

	sField, found := findFieldOnType(fieldName, t.entityType)
	if !found {
		return fmt.Errorf("field %s not found on table type", fieldName)
	}

	if keyKindErr := validateKeyKind(sField.Type.Kind()); keyKindErr != nil {
		return fmt.Errorf("field %s: %s", fieldName, keyKindErr.Error())
	}

	t.indexers[fieldName] = indexer

	return t.validate()
}

func (t *TableSchema) validate() error {
	if t.name == "" {
		return errors.New("name is required")
	}

	for _, i := range t.indexers {
		if err := i.validate(t.entityType); err != nil {
			return fmt.Errorf("invalid indexer %s: %s", i.Field(), err.Error())
		}
	}

	return nil
}
