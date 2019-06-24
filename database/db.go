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
	tables map[string]*Table
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
		if insertErr := table.insert(sliceValue.Index(i)); insertErr != nil {
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
	plan, planErr := planQuery(d, query)

	if planErr != nil {
		return nil, planErr
	}

	result, searchErr := search(d, query, plan)

	return result, searchErr
}

func (d *Db) Explain(query *Query) (string, error) {
	plan, err := planQuery(d, query)
	if err != nil {
		return "query plan error", err
	}
	return plan.dumpPlan(), err
}

func (d *Db) String() string {
	tables := ""
	for name, table := range d.tables {
		indexers := ""
		for _, i := range table.schema.indexers {
			indexers = fmt.Sprintf("%s {%s-len:%v}", indexers, i.Field(), i.Size())
		}
		tables = fmt.Sprintf("%s%s - %v elems - idxs:%s\r\n",
			tables,
			name,
			len(table.elements),
			indexers,
		)
	}
	return fmt.Sprintf("Tables: \r\n\t%s\r\n", tables)
}
