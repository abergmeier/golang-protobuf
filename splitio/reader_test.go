// Go support for Protocol Buffers - Google's data interchange format
//
// Copyright 2019 Andreas Bergmeier.  All rights reserved.
// https://github.com/abergmeier/golang-protobuf
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package splitio

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"
)

var splitReaderTest = []struct {
	name  string
	input []byte
	sep   byte
	lhs   []byte
	rhs   []byte
}{
	{"Simple test", []byte("foo,bar,my\n1,2,3"), []byte("\n")[0], []byte("foo,bar,my"), []byte(`1,2,3`)},
	{"Lefty test", []byte("foo,bar,my\n"), []byte("\n")[0], []byte("foo,bar,my"), []byte{}},
	{"Righty test", []byte("\n1,2,3"), []byte("\n")[0], []byte{}, []byte(`1,2,3`)},
	{"Split first test", []byte("foo,bar,my\n1\n2,3"), []byte("\n")[0], []byte(`foo,bar,my`), []byte("1\n2,3")},
}

// TestNewReadersSequential test splitting of inputs by a separator. Tries
// mostly to find edge cases, where things could go wrong.
func TestNewReadersSequential(t *testing.T) {
	for _, st := range splitReaderTest {

		r := bytes.NewReader(st.input)
		lhsR, rhsR := NewReadersSequential(r, st.sep)

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
