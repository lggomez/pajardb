package main

import (
	"fmt"
	"log"

	"github.com/gofrs/uuid"
)

func GenDataSet(n int) []*Rule {
	dataSet := make([]*Rule, n, n)
	for i := 0; i < n; i++ {
		dataSet[i] = GenerateRule(i, GetUUID())
	}
	return dataSet
}

func GenerateRule(base int, id string) *Rule {
	return &Rule{
		ID:   id,
		Site: fmt.Sprintf("SITE_%v", base%3),
		From: Place{
			Id:   fmt.Sprintf("FROM_%v", base%50),
			Type: fmt.Sprintf("PLACETYPE_%v", base%10),
		},
		To: Place{
			Id:   fmt.Sprintf("TO_%v", base%50),
			Type: fmt.Sprintf("PLACETYPE_%v", base%10),
		},
		Next: Place{
			Id:   fmt.Sprintf("NEXT_%v", base%50),
			Type: fmt.Sprintf("PLACETYPE_%v", base%10),
		},
		Type: fmt.Sprintf("TYPE_%v", base%5),
		Canalization: Canalization{
			Id: fmt.Sprintf("CANAL_%v", base),
			CarrierServices: []CarrierService{
				CarrierService{
					Id: base % 1000,
					ET: []EstimatedTime{
						EstimatedTime{
							ETA: "1800",
							ETD: "1800",
						},
					},
				},
				CarrierService{
					Id: 10000000 - base%1000,
					ET: []EstimatedTime{
						EstimatedTime{
							ETA: "1800",
							ETD: "1800",
						},
					},
				},
			},
		},
		Networks: []int{900 + base%100, 1000 + base%100, 1100 + base%100},
	}
}

func GetUUID() string {
	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID: %v", err)
	}
	return u4.String()
}
