package database

import (
	"testing"
)

func TestNewMetaIndex(t *testing.T) {
	if idx := NewMetaIndex(); idx == nil {
		t.Error("expected non nil value")
	}
}

func TestMetaIndex_Promote(t *testing.T) {
	type fields struct {
		count int
		value string
	}
	tests := []struct {
		name          string
		sut           *MetaIndex
		elements      []fields
		expectedCount int
	}{
		{
			name:          "no_elements",
			sut:           &MetaIndex{index: map[interface{}]byte{}},
			elements:      []fields{},
			expectedCount: 0,
		},
		{
			name: "1_element_single",
			sut:  &MetaIndex{index: map[interface{}]byte{}},
			elements: []fields{
				fields{count: 1, value: "foo"},
			},
			expectedCount: 1,
		},
		{
			name: "1_element_multi",
			sut: &MetaIndex{
				index:      map[interface{}]byte{},
				currentGen: 9,
			},
			elements: []fields{
				fields{count: 10, value: "foo"},
			},
			expectedCount: 1,
		},
		{
			name: "5vs5_element_multi",
			sut: &MetaIndex{
				index:      map[interface{}]byte{},
				currentGen: 9,
			},
			elements: []fields{
				fields{count: 10, value: "foo1"},
				fields{count: 10, value: "foo2"},
				fields{count: 10, value: "foo3"},
				fields{count: 10, value: "foo4"},
				fields{count: 10, value: "foo5"},
				fields{count: 9, value: "foo6"},
				fields{count: 9, value: "foo7"},
				fields{count: 9, value: "foo8"},
				fields{count: 9, value: "foo9"},
				fields{count: 9, value: "foo0"},
			},
			expectedCount: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, e := range tt.elements {
				for i := 0; i < e.count; i++ {
					tt.sut.Inc(e.value)
				}
			}

			tt.sut.Promote()

			if tt.expectedCount != len(tt.sut.index) {
				t.Errorf("expected %v elements, got %v", tt.expectedCount, len(tt.sut.index))
			}
		})
	}
}

// func TestMetaIndex_Inc(t *testing.T) {
// 	type fields struct {
// 		index      map[interface{}]byte
// 		currentGen byte
// 	}
// 	type args struct {
// 		value interface{}
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			g := &MetaIndex{
// 				index:      tt.fields.index,
// 				currentGen: tt.fields.currentGen,
// 			}
// 			g.Inc(tt.args.value)
// 		})
// 	}
// }

// func TestMetaIndex_Delete(t *testing.T) {
// 	type fields struct {
// 		index      map[interface{}]byte
// 		currentGen byte
// 	}
// 	type args struct {
// 		value interface{}
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			g := &MetaIndex{
// 				index:      tt.fields.index,
// 				currentGen: tt.fields.currentGen,
// 			}
// 			g.Delete(tt.args.value)
// 		})
// 	}
// }

func TestMetaIndex_Flush(t *testing.T) {
	type fields struct {
		count int
		value string
	}
	tests := []struct {
		name          string
		sut           *MetaIndex
		elements      []fields
		expectedCount int
	}{
		{
			name:          "no_elements",
			sut:           &MetaIndex{index: map[interface{}]byte{}},
			elements:      []fields{},
			expectedCount: 0,
		},
		{
			name: "1_element_single",
			sut:  &MetaIndex{index: map[interface{}]byte{}},
			elements: []fields{
				fields{count: 1, value: "foo"},
			},
			expectedCount: 1,
		},
		{
			name: "1_element_multi",
			sut: &MetaIndex{
				index:      map[interface{}]byte{},
				currentGen: 9,
			},
			elements: []fields{
				fields{count: 10, value: "foo"},
			},
			expectedCount: 1,
		},
		{
			name: "15_element_multi",
			sut: &MetaIndex{
				index:      map[interface{}]byte{},
				currentGen: 9,
			},
			elements: []fields{
				fields{count: 10, value: "foo1"},
				fields{count: 10, value: "foo2"},
				fields{count: 10, value: "foo3"},
				fields{count: 10, value: "foo4"},
				fields{count: 10, value: "foo5"},
				fields{count: 9, value: "foo6"},
				fields{count: 9, value: "foo7"},
				fields{count: 9, value: "foo8"},
				fields{count: 9, value: "foo9"},
				fields{count: 9, value: "foo0"},
				fields{count: 8, value: "foo11"},
				fields{count: 8, value: "foo12"},
				fields{count: 8, value: "foo13"},
				fields{count: 8, value: "foo14"},
				fields{count: 8, value: "foo15"},
			},
			expectedCount: 15,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, e := range tt.elements {
				for i := 0; i < e.count; i++ {
					tt.sut.Inc(e.value)
				}
			}

			result := tt.sut.Flush()

			if tt.expectedCount != len(result) {
				t.Errorf("expected %v results, got %v", tt.expectedCount, len(result))
			}

			if 0 != len(tt.sut.index) {
				t.Errorf("expected %v elements, got %v", 0, len(tt.sut.index))
			}
		})
	}
}
