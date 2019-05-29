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
