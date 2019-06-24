package main

import (
	"testing"

	"github.com/lggomez/pajardb/database"
)

var rules = GenDataSet(100000)
var dbFull = genDbWithRules()
var testErr error
var searchResult *database.QueryResult
var searchErr error

func genDbWithRules() *database.Db {
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

	db.LoadTableFromSlice("rules", rules)

	return db
}

func genDb() *database.Db {
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

	return db
}

// rm mem.out && rm cpu.out && rm main.test
// go test -cpuprofile=cpu.out -benchmem -memprofile=mem.out -run=^$ -bench=Benchmark_LoadTableFromSlice -v
// pprof -http=:8080 mem.out
func Benchmark_LoadTableFromSlice(b *testing.B) {
	var db *database.Db
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db = genDb()
		b.StartTimer()
		testErr = db.LoadTableFromSlice("rules", rules)
	}
	b.Log(db)
}

func Benchmark_SearchSimple(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		query, _ := database.NewQueryBuilder("rules").
			WithTerm("Site", "SITE_0").
			Build()
		b.StartTimer()
		searchResult, searchErr = dbFull.Search(query)
	}
	b.Logf("RESULTS: %v", searchResult.Len())
}

func Benchmark_SearchMiddle(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		query, _ := database.NewQueryBuilder("rules").
			WithTerm("Site", "SITE_0").
			WithTypedTerm(database.And, "Type", "TYPE_0").
			WithTermIn("From.Id", "FROM_0", "FROM_1", "FROM_2").
			WithTermIn("To.Id", "TO_0", "TO_1", "TO_2").
			Build()
		b.StartTimer()
		searchResult, searchErr = dbFull.Search(query)
	}
	b.Logf("RESULTS: %v", searchResult.Len())
}
