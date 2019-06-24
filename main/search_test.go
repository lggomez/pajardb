package main

import (
	"fmt"
	"testing"

	"github.com/lggomez/pajardb/database"
)

var sTest_rules = genSearchDataSet()

func genSearchDataSet() []*Rule {
	rules := make([]*Rule, 100, 100)

	for i := 0; i < 100; i++ {
		r := &Rule{
			ID:   fmt.Sprintf("RULE_%v", i),
			Site: fmt.Sprintf("SITE_%v", i%2),
			From: Place{
				Id:   fmt.Sprintf("FROM_%v", i%5),
				Type: fmt.Sprintf("PLACETYPE_%v", i%4),
			},
			To: Place{
				Id:   fmt.Sprintf("TO_%v", i%20),
				Type: fmt.Sprintf("PLACETYPE_%v", i%4),
			},
			Next: Place{
				Id:   fmt.Sprintf("NEXT_%v", i%25),
				Type: fmt.Sprintf("PLACETYPE_%v", i%4),
			},
			Type: fmt.Sprintf("TYPE_%v", i%5),
			Canalization: Canalization{
				Id: fmt.Sprintf("CANAL_%v", i),
				CarrierServices: []CarrierService{
					CarrierService{
						Id: i % 1000,
						ET: []EstimatedTime{
							EstimatedTime{
								ETA: "1800",
								ETD: "1800",
							},
						},
					},
					CarrierService{
						Id: 10000000 - i%1000,
						ET: []EstimatedTime{
							EstimatedTime{
								ETA: "1800",
								ETD: "1800",
							},
						},
					},
				},
			},
			Networks: []int{900 + i%100, 1000 + i%100, 1100 + i%100},
		}
		rules[i] = r
	}
	return rules
}

func genSearchDbWithRules() *database.Db {
	schemas := []database.TableSchema{}

	// Create table schema
	ruleTable, schemaErr := database.NewTableSchema("rules", Rule{})
	if schemaErr != nil {
		return nil
	}

	// Create indices
	if idxErr := ruleTable.AddIndex("From.Id"); idxErr != nil {
		return nil
	}
	if idxErr := ruleTable.AddIndex("To.Id"); idxErr != nil {
		return nil
	}
	if idxErr := ruleTable.AddIndex("Next.Id"); idxErr != nil {
		return nil
	}
	if idxErr := ruleTable.AddIndex("From.Type"); idxErr != nil {
		return nil
	}
	if idxErr := ruleTable.AddIndex("To.Type"); idxErr != nil {
		return nil
	}
	if idxErr := ruleTable.AddIndex("Next.Type"); idxErr != nil {
		return nil
	}
	if idxErr := ruleTable.AddIndex("Site"); idxErr != nil {
		return nil
	}
	if idxErr := ruleTable.AddIndex("Type"); idxErr != nil {
		return nil
	}

	schemas = append(schemas, ruleTable)

	// Build DB
	db, dbErr := database.NewDB(schemas)
	if dbErr != nil {
		return nil
	}

	db.LoadTableFromSlice("rules", sTest_rules)

	return db
}

func TestSearch_EveryMainField(t *testing.T) {
	var q *database.Query
	db := genSearchDbWithRules()
	getQb := func(field string, value string) *database.Query {
		query, _ := database.NewQueryBuilder("rules").
			WithTerm(field, value).
			Build()
		return query
	}

	q = getQb("Site", "SITE_0")
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}
	moduloResultAssertion(t, searchResult, 2)

	q = getQb("From.Id", "FROM_0")
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}
	moduloResultAssertion(t, searchResult, 5)

	q = getQb("From.Type", "PLACETYPE_0")
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}
	moduloResultAssertion(t, searchResult, 4)

	q = getQb("To.Id", "TO_0")
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}
	moduloResultAssertion(t, searchResult, 20)

	q = getQb("To.Type", "PLACETYPE_0")
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}
	moduloResultAssertion(t, searchResult, 4)

	q = getQb("Next.Id", "NEXT_0")
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}
	moduloResultAssertion(t, searchResult, 25)

	q = getQb("Next.Type", "PLACETYPE_0")
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}
	moduloResultAssertion(t, searchResult, 4)

	q = getQb("Type", "TYPE_0")
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}
	moduloResultAssertion(t, searchResult, 5)
}

func TestSearch_InFields(t *testing.T) {
	db := genSearchDbWithRules()

	q, queryErr := database.NewQueryBuilder("rules").
		WithTermIn("Site", "SITE_0", "SITE_1").
		Build()
	if queryErr != nil {
		t.Fatalf("unexpected querybuilder error %s", queryErr.Error())
	}
	planExp, expErr := db.Explain(q)
	if expErr != nil {
		t.Fatalf("unexpected query explain error %s", expErr.Error())
	}
	fmt.Printf("QUERY PLAN: \r\n%s\r\n", planExp)
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}

	resultAssertion(t, searchResult, 100)
}

func TestSearch_InFields2(t *testing.T) {
	db := genSearchDbWithRules()

	nextIDs := make([]interface{}, 25, 25)
	for i := 0; i < 25; i++ {
		nextIDs[i] = fmt.Sprintf("NEXT_%v", i)
	}

	q, queryErr := database.NewQueryBuilder("rules").
		WithTermIn("Next.Id", nextIDs...).
		Build()
	if queryErr != nil {
		t.Fatalf("unexpected querybuilder error %s", queryErr.Error())
	}
	planExp, expErr := db.Explain(q)
	if expErr != nil {
		t.Fatalf("unexpected query explain error %s", expErr.Error())
	}
	fmt.Printf("QUERY PLAN: \r\n%s\r\n", planExp)
	searchResult, searchErr = db.Search(q)
	if searchErr != nil {
		t.Fatalf("unexpected search error %s", searchErr.Error())
	}

	resultAssertion(t, searchResult, 100)
}

func moduloResultAssertion(t *testing.T, result *database.QueryResult, mod int) {
	t.Helper()
	resultAssertion(t, result, int(len(sTest_rules)/mod))
}

func resultAssertion(t *testing.T, result *database.QueryResult, expectedCount int) {
	t.Helper()
	if expectedCount != result.Len() {
		t.Errorf("expected %v results, got %v", expectedCount, result.Len())
	}
	for elem := result.Next(); result.HasNext(); elem = result.Next() {
		if elem == nil {
			t.Error("unexpected nil element")
		}
		rule := elem.(Rule)
		if rule.ID == "" {
			t.Error("unexpected default rule value (post-conversion element)")
		}
	}
}
