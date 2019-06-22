package main

import (
	"fmt"
	"time"

	"github.com/lggomez/pajardb/database"
)

func main() {
	start := time.Now()
	schemas := []database.TableSchema{}

	// Create table schema
	ruleTable, schemaErr := database.NewTableSchema("rules", Rule{})
	if schemaErr != nil {
		fmt.Printf("%v", schemaErr.Error())
		return
	}

	// Create indices
	if idxErr := ruleTable.AddIndex("From.Id"); idxErr != nil {
		fmt.Printf("%v", idxErr.Error())
		return
	}
	if idxErr := ruleTable.AddIndex("To.Id"); idxErr != nil {
		fmt.Printf("%v", idxErr.Error())
		return
	}
	if idxErr := ruleTable.AddIndex("Next.Id"); idxErr != nil {
		fmt.Printf("%v", idxErr.Error())
		return
	}
	if idxErr := ruleTable.AddIndex("From.Type"); idxErr != nil {
		fmt.Printf("%v", idxErr.Error())
		return
	}
	if idxErr := ruleTable.AddIndex("To.Type"); idxErr != nil {
		fmt.Printf("%v", idxErr.Error())
		return
	}
	if idxErr := ruleTable.AddIndex("Next.Type"); idxErr != nil {
		fmt.Printf("%v", idxErr.Error())
		return
	}
	if idxErr := ruleTable.AddIndex("Site"); idxErr != nil {
		fmt.Printf("%v", idxErr.Error())
		return
	}
	if idxErr := ruleTable.AddIndex("Type"); idxErr != nil {
		fmt.Printf("%v", idxErr.Error())
		return
	}

	schemas = append(schemas, ruleTable)

	// Build DB
	db, dbErr := database.NewDB(schemas)
	if dbErr != nil {
		fmt.Printf("%v", dbErr.Error())
		return
	}

	rules := GenDataSet(100000)
	if loadErr := db.LoadTableFromSlice("rules", rules); loadErr != nil {
		fmt.Printf("%v", loadErr.Error())
		return
	}

	fmt.Printf("DB CREATED: \r\n%s\r\n", db)

	qb := database.NewQueryBuilder("rules").
		WithTerm("Site", "SITE_0").
		WithTypedTerm(database.And, "Type", "TYPE_0").
		WithTermIn("From.Id", "FROM_0", "FROM_1").
		WithTermIn("To.Id", "TO_0", "TO_1").
		WithTypedTerm(database.Or, "Next.Id", "NEXT_0")
	query, qbErr := qb.Build()
	if qbErr != nil {
		fmt.Printf("%v", qbErr.Error())
		return
	}
	fmt.Printf("QUERY CREATED: \r\n%s\r\n", query)

	planExp, expErr := db.Explain(query)
	if expErr != nil {
		fmt.Printf("%v", qbErr.Error())
		return
	}
	fmt.Printf("QUERY PLAN: \r\n%s\r\n", planExp)

	elapsed := time.Since(start)
	fmt.Printf("Time elapsed %s\r\n", elapsed)
}
