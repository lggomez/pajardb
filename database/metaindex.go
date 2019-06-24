package database

// MetaIndex is an index that will track insertions per inverse index element
// and discard elements below a generation threshold upon promotions
// Promotions can be triggered as required by the consumer
//
// This struct is not goroutine safe and intended for linear processing only
type MetaIndex struct {
	index      map[interface{}]byte
	currentGen byte
}

func NewMetaIndex() *MetaIndex {
	return &MetaIndex{
		index: map[interface{}]byte{},
	}
}

func (g *MetaIndex) Promote() {
	g.currentGen++
	if len(g.index) == 0 {
		return
	}
	for k, v := range g.index {
		if v < g.currentGen {
			delete(g.index, k)
		}
	}
}

func (g *MetaIndex) Inc(value interface{}) {
	if val, ok := g.index[value]; ok {
		g.index[value] = val + 1
	} else {
		g.index[value] = 1
	}
}

func (g *MetaIndex) Dec(value interface{}) {
	if val, ok := g.index[value]; ok {
		g.index[value] = val - 1
	}
}

func (g *MetaIndex) IsInCurrentGeneration(value interface{}) bool {
	if val, ok := g.index[value]; ok && (val == g.currentGen+1) {
		return true
	}
	return false
}

func (g *MetaIndex) IsInLastGeneration(value interface{}) bool {
	if val, ok := g.index[value]; ok && (val == g.currentGen-1) {
		return true
	}
	return false
}

func (g *MetaIndex) Delete(value interface{}) {
	delete(g.index, value)
}

func (g *MetaIndex) Flush() []PtrValue {
	result := make([]PtrValue, len(g.index), len(g.index))
	var i int
	for k, _ := range g.index {
		result[i] = k
		delete(g.index, k)
		i++
	}
	return result
}
