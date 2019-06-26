package database

// MetaIndex is an index that will track insertions per inverse index element
// and discard elements below a target
//
// This struct is not goroutine safe and intended for linear processing only
type MetaIndex struct {
	index  map[interface{}]int
	target int
}

func NewMetaIndex(plan *queryPlan) *MetaIndex {
	return &MetaIndex{
		index:  make(map[interface{}]int, plan.getMaxCardinality()),
		target: len(plan.orderedSteps),
	}
}

func (g *MetaIndex) Inc(value interface{}) {
	g.index[value]++
}

func (g *MetaIndex) Dec(value interface{}) {
	g.index[value]--
}

func (g *MetaIndex) Flush() []PtrValue {
	result := make([]PtrValue, 0, len(g.index))
	for k, val := range g.index {
		if val >= g.target {
			result = append(result, k)
		}
	}
	return result
}
