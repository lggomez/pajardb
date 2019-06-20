package database

import (
	"errors"
)

type searchOp struct {
	values []StructValue
	index  *BaseIndexer
}

type queryStep struct {
	termType   TermType
	operations []*searchOp
}

type queryPlan struct {
	orderedSteps []*queryStep
}

func planQuery(db *Db, query *Query) (*queryPlan, error) {
	table := db.tables[query.table]
	plan := &queryPlan{orderedSteps: []*queryStep{}}

	if len(query.terms) == 0 {
		return plan, errors.New("query must have search terms")
	}

	// First pass: group terms by type
	prevType := query.terms[0].termType
	currentStep := queryStep{
		termType:   prevType,
		operations: []*searchOp{},
	}
	for _, t := range query.terms {
		// End of previous group
		if t.termType != prevType {
			prevType = t.termType
			newStep := &currentStep
			plan.orderedSteps = append(plan.orderedSteps, newStep)
			currentStep = queryStep{
				termType:   t.termType,
				operations: []*searchOp{},
			}
		}

		// Group operations
		for _, p := range t.params {
			index := table.schema.indexers[p.FieldName]
			op := &searchOp{
				index:  index,
				values: []StructValue{},
			}
			currentStep.operations = append(currentStep.operations, op)
		}
	}

	return plan, nil
}

func (p *queryPlan) dumpPlan() string {
	str := ""

	return str
}
