// Go support for Protocol Buffers - Google's data interchange format
//
// Copyright 2019 Andreas Bergmeier.  All rights reserved.
// https://github.com/golang/protobuf
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
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"

	pb "github.com/abergmeier/protobuf/csvpb/csvpb_test_proto"
)

var (

	simpleObject = &pb.Simple{
		OInt32:     proto.Int32(-32),
		OInt32Str:  proto.Int32(-32),
		OInt64:     proto.Int64(-6400000000),
		OInt64Str:  proto.Int64(-6400000000),
		OUint32:    proto.Uint32(32),
		OUint32Str: proto.Uint32(32),
		OUint64:    proto.Uint64(6400000000),
		OUint64Str: proto.Uint64(6400000000),
		OSint32:    proto.Int32(-13),
		OSint32Str: proto.Int32(-13),
		OSint64:    proto.Int64(-2600000000),
		OSint64Str: proto.Int64(-2600000000),
		OFloat:     proto.Float32(3.14),
		OFloatStr:  proto.Float32(3.14),
		ODouble:    proto.Float64(6.02214179e23),
		ODoubleStr: proto.Float64(6.02214179e23),
		OBool:      proto.Bool(true),
		OString:    proto.String("hello \"there\""),
		OBytes:     []byte("beep boop"),
	}

	simpleInputCSV = `oBool,oInt32,oInt32Str,oInt64,oInt64Str,oUint32,oUint32Str,oUint64,oUint64Str,oSint32,oSint32Str,oSint64,oSint64Str,oFloat,oFloatStr,oDouble,oDoubleStr,oString,oBytes
` +
		`true,` +
		`-32,` +
		`"-32",` +
		`-6400000000,` +
		`"-6400000000",` +
		`32,` +
		`"32",` +
		`6400000000,` +
		`"6400000000",` +
		`-13,` +
		`"-13",` +
		`-2600000000,` +
		`"-2600000000",` +
		`3.14,` +
		`"3.14",` +
		`6.02214179e+23,` +
		`"6.02214179e+23",` +
		`"hello ""there""",` +
		`"YmVlcCBib29w"`

	repeatsObject = &pb.Repeats{
		RBool:   []bool{true, false, true},
		RInt32:  []int32{-3, -4, -5},
		RInt64:  []int64{-123456789, -987654321},
		RUint32: []uint32{1, 2, 3},
		RUint64: []uint64{6789012345, 3456789012},
		RSint32: []int32{-1, -2, -3},
		RSint64: []int64{-6789012345, -3456789012},
		RFloat:  []float32{3.14, 6.28},
		RDouble: []float64{299792458 * 1e20, 6.62606957e-34},
		RString: []string{"happy", "days"},
		RBytes:  [][]byte{[]byte("skittles"), []byte("m&m's")},
	}

	repeatsObjectCSV = `rBool,rInt32,rInt64,rUint32,rUint64,rSint32,rSint64,rFloat,rDouble,rString,rBytes
` +
		`"true,false,true",` +
		`"-3,-4,-5",` +
		`"-123456789,-987654321",` +
		`"1,2,3",` +
		`"6789012345,3456789012",` +
		`"-1,-2,-3",` +
		`"-6789012345,-3456789012",` +
		`"3.14,6.28",` +
		`"2.99792458e+28,6.62606957e-34",` +
		`"happy,days",` +
		`"c2tpdHRsZXM=,bSZtJ3M="`
)

var unmarshalingTests = []struct {
	desc        string
	unmarshaler Unmarshaler
	csv         string
	pb          proto.Message
}{
	{"simple flat object", Unmarshaler{}, simpleInputCSV, simpleObject},
	{"repeated fields flat object", Unmarshaler{}, repeatsObjectCSV, repeatsObject},
}

func TestUnmarshaling(t *testing.T) {
	for _, tt := range unmarshalingTests {
		// Make a new instance of the type of our expected object.
		p := reflect.New(reflect.TypeOf(tt.pb).Elem()).Interface().(proto.Message)

		err := tt.unmarshaler.Unmarshal(strings.NewReader(tt.csv), p)
		if err != nil {
			t.Errorf("unmarshalling %s: %v", tt.desc, err)
			continue
		}

		// For easier diffs, compare text strings of the protos.
		exp := proto.MarshalTextString(tt.pb)
		act := proto.MarshalTextString(p)
		if string(exp) != string(act) {
			t.Errorf("%s: got [%s] want [%s]", tt.desc, act, exp)
		}
	}
}
