package database

import (
	"reflect"
)

type QueryResult struct {
	results []PtrValue
	current int
}

func (qr *QueryResult) HasNext() bool {
	return qr.current < len(qr.results)
}

func (qr *QueryResult) Next() interface{} {
	if len(qr.results) == 0 {
		return nil
	}
	val := reflect.Indirect(reflect.ValueOf(qr.results[qr.current])).Interface()
	qr.current = qr.current + 1
	return val
}

func (qr *QueryResult) Len() int {
	return len(qr.results)
}
