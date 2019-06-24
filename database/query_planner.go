package database

import (
	"errors"
	"fmt"
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

	// Process and group terms by type (except In, Not) into steps
	var currentStep *queryStep
	var prevType TermType
	for i, t := range query.terms {
		// Step transitions
		if i == 0 {
			prevType = t.termType
			currentStep = &queryStep{
				termType:   t.termType,
				operations: []*searchOp{},
			}
			plan.orderedSteps = append(plan.orderedSteps, currentStep)
		} else if (t.termType != prevType) || ((t.termType == In) || (t.termType == Not)) {
			prevType = t.termType
			plan.orderedSteps = append(plan.orderedSteps, currentStep)
			currentStep = &queryStep{
				termType:   t.termType,
				operations: []*searchOp{},
			}
		}

		// Group operations
		for _, p := range t.params {
			index := table.schema.indexers[p.FieldName]
			op := &searchOp{
				index:  index,
				values: []StructValue{p.Value},
			}
			currentStep.operations = append(currentStep.operations, op)
		}
	}

	return plan, nil
}

func (p *queryPlan) dumpPlan() string {
	str := ""

	for i, s := range p.orderedSteps {
		stepStr := fmt.Sprintf("type:'%s' operands:\r\n\t", s.termType)
		for _, op := range s.operations {
			stepStr = fmt.Sprintf("%s idx:%s - %+v\r\n\t", stepStr, op.index.fieldName, op.values)
		}
		str = fmt.Sprintf("%s step %v -> %s\r\n", str, i+1, stepStr)
	}

	return str
}
