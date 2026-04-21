package hotelworkflow

import (
	"reflect"
	"testing"
)

func TestHotelMustDateRange(t *testing.T) {
	got, ok := hotelMustDateRange("2015-04-09", "2015-04-12")
	if !ok {
		t.Fatalf("hotelMustDateRange returned !ok")
	}

	want := []string{"2015-04-09", "2015-04-10", "2015-04-11"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hotelMustDateRange = %#v, want %#v", got, want)
	}
}

func TestHotelRequestIDStringPreservesNestedIDs(t *testing.T) {
	got := hotelRequestIDString("123/reservation")
	if got != "123/reservation" {
		t.Fatalf("hotelRequestIDString = %q, want %q", got, "123/reservation")
	}
}
