package main

import (
	"testing"

	"github.com/lggomez/pajardb/database"
)

var db = genDb()
var rules = GenDataSet(100000)
var testErr error

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

func Benchmark_LoadTableFromSlice(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		testErr = db.LoadTableFromSlice("rules", rules)
	}
}
