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

	fmt.Printf("created db \r\n%s", db)

	qb := database.NewQueryBuilder("rules").
		WithTerm("Site", "SITE_0").
		WithTerm("Type", "TYPE_0")
	query, qbErr := qb.Build()
	if qbErr != nil {
		fmt.Printf("%v", qbErr.Error())
		return
	}

	fmt.Printf("created query \r\n%s", query)
	elapsed := time.Since(start)
	fmt.Printf("Time elapsed %s", elapsed)
}
