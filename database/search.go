package database

func search(db *Db, query *Query, plan *queryPlan) (*QueryResult, error) {
	metaIndex := NewMetaIndex(plan)

	for _, step := range plan.orderedSteps {
		for i, op := range step.operations {
			if i > 0 && step.termType == And {
				metaIndex.target = metaIndex.target + 1
			}
			for _, key := range op.values {
				for _, elem := range op.indexer.index[key] {
					if step.termType == Not {
						metaIndex.Dec(elem)
					} else {
						metaIndex.Inc(elem)
					}
				}
			}
		}
	}

	result := &QueryResult{
		results: metaIndex.Flush(),
	}

	return result, nil
}
