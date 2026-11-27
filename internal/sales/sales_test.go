package sales

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEvaluateSalePrefixPartialFullAndRejections(t *testing.T) {
	t0 := time.Date(2026, 11, 25, 15, 0, 0, 0, time.UTC)
	sale := prefixSaleEvent{EventID: uuid.New(), EventType: "sale", StoreID: "store-1", OccurredAt: t0, AmountMinor: 1000}
	dupSale := prefixSaleEvent{EventID: uuid.New(), EventType: "sale", StoreID: "store-1", OccurredAt: t0.Add(time.Minute), AmountMinor: 1000}
	ret1 := prefixSaleEvent{EventID: uuid.New(), EventType: "return", StoreID: "store-2", OccurredAt: t0.Add(2 * time.Minute), AmountMinor: 400} // cross-store allowed
	voidAfterRet := prefixSaleEvent{EventID: uuid.New(), EventType: "void", StoreID: "store-1", OccurredAt: t0.Add(3 * time.Minute), AmountMinor: 1000}
	overRet := prefixSaleEvent{EventID: uuid.New(), EventType: "return", StoreID: "store-1", OccurredAt: t0.Add(4 * time.Minute), AmountMinor: 700}
	ret2 := prefixSaleEvent{EventID: uuid.New(), EventType: "return", StoreID: "store-1", OccurredAt: t0.Add(5 * time.Minute), AmountMinor: 600}
	retAfterFull := prefixSaleEvent{EventID: uuid.New(), EventType: "return", StoreID: "store-1", OccurredAt: t0.Add(6 * time.Minute), AmountMinor: 100}

	status, returned, res := evaluateSalePrefix([]prefixSaleEvent{sale, dupSale, ret1, voidAfterRet, overRet, ret2, retAfterFull}, &sale)
	if status != "FULLY_RETURNED" || returned != 1000 {
		t.Fatalf("got status=%s returned=%d, want FULLY_RETURNED/1000", status, returned)
	}
	wantReasons := map[int]string{
		1: "DUPLICATE_SALE_ID",
		3: "CANNOT_VOID_RETURNED_SALE",
		4: "RETURN_EXCEEDS_REMAINING",
		6: "ALREADY_FULLY_RETURNED",
	}
	for idx, want := range wantReasons {
		if res[idx].newStatus != "REJECTED_INVALID" || res[idx].rejectCode == nil || *res[idx].rejectCode != want {
			t.Fatalf("idx %d: got status=%s code=%v, want REJECTED_INVALID/%s", idx, res[idx].newStatus, res[idx].rejectCode, want)
		}
	}
}

func TestEvaluateSalePrefixVoidAndPostVoidRejections(t *testing.T) {
	t0 := time.Date(2026, 11, 25, 15, 0, 0, 0, time.UTC)
	sale := prefixSaleEvent{EventID: uuid.New(), EventType: "sale", StoreID: "store-1", OccurredAt: t0, AmountMinor: 1000}
	wrongStoreVoid := prefixSaleEvent{EventID: uuid.New(), EventType: "void", StoreID: "store-2", OccurredAt: t0.Add(time.Minute), AmountMinor: 1000}
	wrongAmtVoid := prefixSaleEvent{EventID: uuid.New(), EventType: "void", StoreID: "store-1", OccurredAt: t0.Add(2 * time.Minute), AmountMinor: 500}
	okVoid := prefixSaleEvent{EventID: uuid.New(), EventType: "void", StoreID: "store-1", OccurredAt: t0.Add(3 * time.Minute), AmountMinor: 1000}
	dupVoid := prefixSaleEvent{EventID: uuid.New(), EventType: "void", StoreID: "store-1", OccurredAt: t0.Add(4 * time.Minute), AmountMinor: 1000}
	retAfterVoid := prefixSaleEvent{EventID: uuid.New(), EventType: "return", StoreID: "store-1", OccurredAt: t0.Add(5 * time.Minute), AmountMinor: 200}

	status, returned, res := evaluateSalePrefix([]prefixSaleEvent{sale, wrongStoreVoid, wrongAmtVoid, okVoid, dupVoid, retAfterVoid}, &sale)
	if status != "VOIDED" || returned != 0 {
		t.Fatalf("got status=%s returned=%d, want VOIDED/0", status, returned)
	}
	if res[1].rejectCode == nil || *res[1].rejectCode != "VOID_STORE_MISMATCH" {
		t.Fatalf("expected VOID_STORE_MISMATCH, got %+v", res[1])
	}
	if res[2].rejectCode == nil || *res[2].rejectCode != "INVALID_AMOUNT" {
		t.Fatalf("expected INVALID_AMOUNT, got %+v", res[2])
	}
	if res[3].newStatus != "APPLIED" || res[3].signedDelta != -1000 {
		t.Fatalf("expected APPLIED/-1000 for valid void, got %+v", res[3])
	}
	if res[4].rejectCode == nil || *res[4].rejectCode != "ALREADY_VOIDED" {
		t.Fatalf("expected ALREADY_VOIDED, got %+v", res[4])
	}
	if res[5].rejectCode == nil || *res[5].rejectCode != "CANNOT_RETURN_VOIDED_SALE" {
		t.Fatalf("expected CANNOT_RETURN_VOIDED_SALE, got %+v", res[5])
	}
}
