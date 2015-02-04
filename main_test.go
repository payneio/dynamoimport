package main

import (
	"fmt"
	"testing"
)

func TestGetItem(t *testing.T) {
	s := "5372633911M5372614262510102SALON SHOES20SALON SHOES80BOOTS822TALL SHAFTACCUMULATEACCUMULATESADOLD220169643906STUART WEITZMANACCUMULA"
	configure()
	postItem, err := getItem(s)
	if err != nil {
		t.Error(err)
	}
	if *postItem["SKU"].S != "53726339" {
		t.Error("Expected a sku, got ", *postItem["SKU"].S)
	}
	if *postItem["VendorProductNumber"].S != "ACCUMULA" {
		t.Error("Expected a VPN, got ", *postItem["VendorProductNumber"].S)
	}
	if _, ok := postItem["RecommendRetail"]; ok {
		t.Error("Expected no RecommendRetail, got one: ", *postItem["RecommendRetail"].S)
	}
}

func TestGetLine(t *testing.T) {
	s := "5372633911M5372614262510102SALON SHOES20SALON SHOES80BOOTS822TALL SHAFTACCUMULATEACCUMULATESADOLD220169643906STUART WEITZMANACCUMULA"
	configure()
	postItem, err := getItem(s)
	if err != nil {
		fmt.Errorf(err.Error())
	}
	s2, err := getLine(postItem)
	if s != s2 {
		fmt.Errorf("Expected: %s, Got: %s", s, s2)
	}
}
