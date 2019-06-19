package main

import (
	"context"
)

type FilterConditions struct {
	Site                           string
	Service                        int
	AnyDistributionCarrierServices []CarrierService
}

type RulesSearcher interface {
	FetchRulesFromToIn(context.Context, []string, []string, FilterConditions) (Rules, error)
}

// Type size: 208 bytes
//
// struct {
// 	ID   string
// 	Site string
// 	From struct {
//         Id   string
//         Type string
//     }
//     To struct {
//         Id   string
//         Type string
//     }
//     Next struct {
//         Id   string
//         Type string
//     }
//     Type         string
//     Canalization struct {
//         Id   string
//         CarrierServices [] struct {
//             Id   string
//             ET [] struct {
//                 ETA   string
//                 ETD string
//             }
//         }
//     }
// 	Networks     []int
// }
//
type Rule struct {
	ID           string       `json:"id"`
	Site         string       `json:"site"`
	From         Place        `json:"from"`
	To           Place        `json:"to"`
	Next         Place        `json:"next,omitempty"`
	Type         string       `json:"type"`
	Canalization Canalization `json:"canalization"`
	Networks     []int        `json:"networks"`
}
type Canalization struct {
	Id              string           `json:"id"`
	CarrierServices []CarrierService `json:"carrier_services,omitempty"`
}
type CarrierService struct {
	Id int             `json:"id"`
	ET []EstimatedTime `json:"et,omitempty"`
}
type EstimatedTime struct {
	ETA string `json:"eta,omitempty"`
	ETD string `json:"etd,omitempty"`
}

//-------Places

type Place struct {
	Id   string `json:"id"`
	Type string `json:"type"`
}

//-------Rules
type Rules []Rule

type ScrollableRules struct {
	Rules    Rules
	ScrollID string
}
