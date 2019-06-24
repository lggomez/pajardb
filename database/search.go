package database

func search(db *Db, query *Query, plan *queryPlan) (*QueryResult, error) {
	metaIndex := NewMetaIndex()

	for _, step := range plan.orderedSteps {
		for _, op := range step.operations {
			for _, key := range op.values {
				for _, elem := range op.index.index[key] {
					if step.termType == Or && metaIndex.IsInLastGeneration(elem) {
						// Inclusively count element for or terms
						metaIndex.Inc(elem)
					} else {
						metaIndex.Inc(elem)
					}
				}
			}
		}
		metaIndex.Promote()
	}

	result := &QueryResult{
		results: metaIndex.Flush(),
	}

	return result, nil
}
