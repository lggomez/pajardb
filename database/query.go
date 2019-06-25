package database

import (
	"errors"
	"fmt"
)

type QueryType uint

const (
	// TODO: add scroll support
	Fetch QueryType = iota
)

func (qt QueryType) String() string {
	switch qt {
	case Fetch:
		return "Fetch"
	default:
		return "Fetch"
	}
}

type TermType uint

const (
	And TermType = iota
	Or
	Not
	In
)

func (t TermType) String() string {
	switch t {
	case Or:
		return "Or"
	case Not:
		return "Not"
	case In:
		return "In"
	case And:
		fallthrough
	default:
		return "And"
	}
}

type indexParam struct {
	FieldName string
	Value     StructValue
}

type queryTerm struct {
	termType TermType
	params   []indexParam
}

type Query struct {
	qtype QueryType
	table string
	terms []*queryTerm
}

func (q *Query) String() string {
	str := ""
	for i, term := range q.terms {
		termStr := fmt.Sprintf("type:'%s' terms:", term.termType)
		sep := ""
		if len(term.params) > 1 {
			sep = ","
		}
		for fieldValue, value := range term.params {
			termStr = fmt.Sprintf("%s{%+v - %+v}%s", termStr, fieldValue, value, sep)
		}
		str = fmt.Sprintf("%s%v:[%s]\r\n\t",
			str,
			i,
			termStr,
		)
	}
	return fmt.Sprintf("Query - table %s:\r\n\t%s\r\n", q.table, str)
}

type QueryBuilder struct {
	table string
	terms []*queryTerm
}

func NewQueryBuilder(table string) *QueryBuilder {
	return &QueryBuilder{
		table: table,
	}
}

func (qb *QueryBuilder) WithTerm(fieldName string, value interface{}) *QueryBuilder {
	return qb.WithTypedTerm(And, fieldName, value)
}

func (qb *QueryBuilder) WithTermIn(fieldName string, values ...interface{}) *QueryBuilder {
	t := &queryTerm{
		termType: In,
		params:   []indexParam{},
	}
	for _, v := range values {
		t.params = append(t.params, indexParam{FieldName: fieldName, Value: v})
	}
	qb.terms = append(qb.terms, t)
	return qb
}

func (qb *QueryBuilder) WithTypedTerm(termType TermType, fieldName string, value interface{}) *QueryBuilder {
	t := &queryTerm{
		termType: termType,
		params:   []indexParam{},
	}
	t.params = append(t.params, indexParam{FieldName: fieldName, Value: value})
	qb.terms = append(qb.terms, t)
	return qb
}

func (qb *QueryBuilder) WithTypedTerms(termType TermType, fieldName string, values ...interface{}) *QueryBuilder {
	t := &queryTerm{
		termType: termType,
		params:   []indexParam{},
	}
	for _, v := range values {
		t.params = append(t.params, indexParam{FieldName: fieldName, Value: v})
	}
	qb.terms = append(qb.terms, t)
	return qb
}

func (qb *QueryBuilder) validate() error {
	for _, qt := range qb.terms {
		for _, param := range qt.params {
			if param.FieldName == "" {
				return errors.New("field name required")
			}
			if param.Value == nil {
				return errors.New("query value cannot be nil")
			}
		}
	}
	return nil
}

func (qb *QueryBuilder) Build() (*Query, error) {
	if err := qb.validate(); err != nil {
		return nil, err
	}
	return &Query{
		qtype: Fetch,
		table: qb.table,
		terms: qb.terms,
	}, nil
}
