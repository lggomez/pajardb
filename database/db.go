package database

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type StructValue interface{}
type PtrValue interface{}

type Db struct {
	disableOptimizer bool
	tables           map[string]*Table
	mu               sync.RWMutex
}

func NewDB(ss []TableSchema) (*Db, error) {
	db := &Db{
		tables: map[string]*Table{},
	}

	for _, schema := range ss {
		if err := db.addTable(schema); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (d *Db) LoadTableFromSlice(tableName string, elements interface{}) error {
	//println(reflect.ValueOf(elements).String())
	sliceValue := reflect.ValueOf(elements)
	sliceType := reflect.TypeOf(elements)
	kind := sliceType.Kind()

	if (kind != reflect.Slice) && (kind != reflect.Array) {
		return fmt.Errorf("elements: must be slice or array type, got %s (%s)", kind, sliceType)
	}

	if sliceValue.IsNil() || !sliceValue.IsValid() {
		return errors.New("elements: nil or invalid value")
	}

	table, found := d.tables[tableName]
	if !found {
		return fmt.Errorf("table %s not found", tableName)
	}

	size := sliceValue.Len()
	for i := 0; i < size; i++ {
		e := sliceValue.Index(i)
		var elPtr PtrValue
		if e.Kind() != reflect.Ptr {
			if !e.CanAddr() {
				return errors.New("elements: CanAddr == false on slice element")
			}
			elPtr = e.Addr().Interface()
		} else {
			elPtr = e.Interface()
		}
		if insertErr := table.insert(elPtr); insertErr != nil {
			return fmt.Errorf("insert error: %s", insertErr.Error())
		}
	}

	return nil
}

func (d *Db) addTable(s TableSchema) error {
	if err := s.validate(); err != nil {
		return fmt.Errorf("error validating table: %s", err.Error())
	}

	t := &Table{
		elements: []PtrValue{},
		schema:   s,
		lock:     &sync.RWMutex{},
	}
	d.tables[s.name] = t

	return nil
}

func (d *Db) Search(query *Query) (*QueryResult, error) {
	result := &QueryResult{}

	return result, nil
}

func (d *Db) String() string {
	tables := ""
	for name, table := range d.tables {
		indexers := ""
		for _, i := range table.schema.indexers {
			indexers = fmt.Sprintf("%s {%s-len:%v}", indexers, i.Field(), i.Len())
		}
		tables = fmt.Sprintf("%s%s - %v elems - idxs:%s\r\n",
			tables,
			name,
			len(table.elements),
			indexers,
		)
	}
	return fmt.Sprintf("Tables: \r\n\t%s", tables)
}