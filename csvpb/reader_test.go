package csvpb

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"
)

var splitReaderTest = []struct {
	name string
	input []byte
	sep byte
	lhs []byte
	rhs []byte
}{
	{"Simple test", []byte("foo,bar,my\n1,2,3"), []byte("\n")[0], []byte("foo,bar,my"), []byte(`1,2,3`)},
	{"Lefty test", []byte("foo,bar,my\n"), []byte("\n")[0], []byte("foo,bar,my"), []byte{}},
	{"Righty test", []byte("\n1,2,3"), []byte("\n")[0], []byte{}, []byte(`1,2,3`)},
	{"Split first test", []byte("foo,bar,my\n1\n2,3"), []byte("\n")[0], []byte(`foo,bar,my`), []byte("1\n2,3")},
}

// TestSplitReaderSequential test splitting of inputs by a separator. Tries
// mostly to find edge cases, where things could go wrong.
func TestSplitReaderSequential(t *testing.T) {
	for _, st := range splitReaderTest {

		r := bytes.NewReader(st.input)
		lhsR, rhsR := SplitReaderSequential(r, st.sep)

		lhs, err := ioutil.ReadAll(lhsR)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(lhs, st.lhs) {
			t.Fatalf("Unexpected: got %v, expected %v", lhs, st.lhs)
		}

		rhs, err := ioutil.ReadAll(rhsR)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(rhs, st.rhs) {
			t.Fatalf("Unexpected: got %v, expected %v", rhs, st.rhs)
		}
	}
}
