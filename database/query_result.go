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
	val := reflect.Indirect(reflect.ValueOf(qr.results[qr.current])).Interface()
	qr.current = qr.current + 1
	return val
}