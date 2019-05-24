package csvpb

import (
	"reflect"
	"testing"
	"strings"
)

func TestEmptyDecode(t *testing.T) {
	d := NewDecoder(strings.NewReader(""))
	if d.More() {
		t.Fatal("More() lies")
	}
}

func TestSingleDecode(t *testing.T) {
	d := NewDecoder(strings.NewReader("foo\nbar"))
	if !d.More() {
		t.Fatal("More() lies")
	}

	v, err := d.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if v == nil && len(v) != 1 {
		t.Fatalf("Unexpected decoded value: %v", v)
	}
}

func TestDoubleDecode(t *testing.T) {
	d := NewDecoder(strings.NewReader("foo,0\nbar,1\nmy,2"))
	if !d.More() {
		t.Fatal("First More() lies")
	}

	v, err := d.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v, []string{"foo", "0"}) {
		t.Fatalf("Value wrong %v", v)
	}

	if !d.More() {
		t.Fatal("Second More() lies")
	}

	v, err = d.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v, []string{"bar", "1"}) {
		t.Fatalf("Value wrong %v", v)
	}

	if !d.More() {
		t.Fatal("Third More() lies")
	}

	v, err = d.Decode()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(v, []string{"my", "2"}) {
		t.Fatalf("Value wrong %v", v)
	}

	if d.More() {
		t.Fatal("Fourth More() lies")
	}
}
