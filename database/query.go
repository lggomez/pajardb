package database

import (
	"fmt"
)

const iota = 0
type QueryType uint

const (
	// TODO: add scroll support
	Fetch QueryType = iota
)

func (qt QueryType) String() string {
	switch qt {
	case iota:
		fallthrough
	default:
		return "Fetch"
	}
}

type TermType uint

const (
	And TermType = iota
	Or
	Not
)

func (t TermType) String() string {
	switch t {
	case iota +1:
		return "Or"
	case iota +2:
		return "Not"
	case iota:
		fallthrough
	default:
		return "And"
	}
}

type QueryTermGroup map[string]interface{}
type termGroup map[string]StructValue

type queryTerm struct {
	termType  TermType
	fieldName string
	group     termGroup
}

type Query struct {
	qtype QueryType
	table string
	terms []queryTerm
}

func (q *Query) String() string {
	str := ""
	for i, term := range q.terms {
		termStr := fmt.Sprintf("type:'%s' terms:", term.termType)
		sep := ""
		if len(term.group) > 1 {
			sep = ","
		}
		for fieldValue, value := range term.group {
			termStr = fmt.Sprintf("%s{%s - %s}%s", termStr, fieldValue, value, sep)
		}
		str = fmt.Sprintf("%s%v:[%s]\r\n\t",
			str,
			i,
			termStr,
		)
	}
	return fmt.Sprintf("Query - table %s:\r\n\t%s", q.table, str)
}

type QueryBuilder struct {
	table string
	terms []queryTerm
}

func NewQueryBuilder(table string) *QueryBuilder {
	return &QueryBuilder{
		table: table,
	}
}

func (qb *QueryBuilder) WithTerm(fieldName string, value interface{}) *QueryBuilder {
	return qb.WithTypedTerm(And, fieldName, value)
}

func (qb *QueryBuilder) WithTypedTerm(termType TermType, fieldName string, value interface{}) *QueryBuilder {
	t := queryTerm{
		termType: termType,
		group: termGroup{},
	}
	t.group[fieldName] = value
	qb.terms = append(qb.terms, t)
	return qb
}

func (qb *QueryBuilder) WithQueryTermGroup(terms QueryTermGroup) *QueryBuilder {
	return qb.WithTypedQueryTermGroup(And, terms)
}

func (qb *QueryBuilder) WithTypedQueryTermGroup(termType TermType, terms QueryTermGroup) *QueryBuilder {
	t := queryTerm{
		termType: termType,
		group: termGroup{},
	}
	for k, v := range terms {
		t.group[k] = v
	}
	qb.terms = append(qb.terms, t)
	return qb
}

func (qb *QueryBuilder) validate() error {
	for _, qt := range qb.terms {
		for fieldName, value := range qt.group {
			if fieldName == "" {
				return fmt.Errorf("field name required")
			}
			if value == nil {
				return fmt.Errorf("query value cannot be nil")
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
