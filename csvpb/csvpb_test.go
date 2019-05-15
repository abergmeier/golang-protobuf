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
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"

	pb "github.com/abergmeier/protobuf/csvpb/csvpb_test_proto"
	stpb "github.com/golang/protobuf/ptypes/struct"
	wpb "github.com/golang/protobuf/ptypes/wrappers"
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

	innerSimple   = &pb.Simple{OInt32: proto.Int32(-32)}
	innerSimple2  = &pb.Simple{OInt64: proto.Int64(25)}
	innerRepeats  = &pb.Repeats{RString: []string{"roses", "red"}}
	innerRepeats2 = &pb.Repeats{RString: []string{"violets", "blue"}}
	enumObject = &pb.Widget{
		Color:    pb.Widget_GREEN.Enum(),
		RColor:   []pb.Widget_Color{pb.Widget_RED, pb.Widget_GREEN, pb.Widget_BLUE},
	}

	enumObjectCSV = `color,rColor
` +
		`GREEN,` +
		`"RED,GREEN,BLUE"`

	realNumber     = &pb.Real{Value: proto.Float64(3.14159265359)}
	realNumberName = "Pi"
	complexNumber  = &pb.Complex{Imaginary: proto.Float64(0.5772156649)}

	nonFinites = &pb.NonFinites{
		FNan:  proto.Float32(float32(math.NaN())),
		FPinf: proto.Float32(float32(math.Inf(1))),
		FNinf: proto.Float32(float32(math.Inf(-1))),
		DNan:  proto.Float64(float64(math.NaN())),
		DPinf: proto.Float64(float64(math.Inf(1))),
		DNinf: proto.Float64(float64(math.Inf(-1))),
	}
	nonFinitesCSV = `fNan,fPinf,fNinf,dNan,dPinf,dNinf
` +
		`NaN,` +
		`Infinity,` +
		`-Infinity,` +
		`NaN,` +
		`Infinity` +
		`-Infinity`
)

var unmarshalingTests = []struct {
	desc        string
	unmarshaler Unmarshaler
	csv         string
	pb          proto.Message
}{
	{"simple flat object", Unmarshaler{}, simpleInputCSV, simpleObject},
	{"repeated fields flat object", Unmarshaler{}, repeatsObjectCSV, repeatsObject},
	{"nested enum flat object", Unmarshaler{}, enumObjectCSV, enumObject},
	{"enum-string object", Unmarshaler{}, "color\nBLUE", &pb.Widget{Color: pb.Widget_BLUE.Enum()}},
	{"enum-value object", Unmarshaler{}, "color\n 2", &pb.Widget{Color: pb.Widget_BLUE.Enum()}},
	{"unknown field with allowed option", Unmarshaler{AllowUnknownFields: true}, "unknown\nfoo", new(pb.Simple)},

	{"null Value", Unmarshaler{}, "val\n\"\"", &pb.KnownTypes{Val: &stpb.Value{Kind: &stpb.Value_NullValue{stpb.NullValue_NULL_VALUE}}}},
	{"bool Value", Unmarshaler{}, "val\ntrue", &pb.KnownTypes{Val: &stpb.Value{Kind: &stpb.Value_BoolValue{true}}}},
	{"string Value", Unmarshaler{}, "val\nx", &pb.KnownTypes{Val: &stpb.Value{Kind: &stpb.Value_StringValue{"x"}}}},
	{"string number value", Unmarshaler{}, "val\n\"9223372036854775807\"", &pb.KnownTypes{Val: &stpb.Value{Kind: &stpb.Value_StringValue{"9223372036854775807"}}}},

	{"DoubleValue", Unmarshaler{}, "dbl\n1.2", &pb.KnownTypes{Dbl: &wpb.DoubleValue{Value: 1.2}}},
	{"FloatValue", Unmarshaler{}, "flt\n1.2", &pb.KnownTypes{Flt: &wpb.FloatValue{Value: 1.2}}},
	{"Int64Value", Unmarshaler{}, "i64\n-3", &pb.KnownTypes{I64: &wpb.Int64Value{Value: -3}}},
	{"UInt64Value", Unmarshaler{}, "u64\n3", &pb.KnownTypes{U64: &wpb.UInt64Value{Value: 3}}},
	{"Int32Value", Unmarshaler{}, "i32\n-4", &pb.KnownTypes{I32: &wpb.Int32Value{Value: -4}}},
	{"UInt32Value", Unmarshaler{}, "u32\n4", &pb.KnownTypes{U32: &wpb.UInt32Value{Value: 4}}},
	{"BoolValue", Unmarshaler{}, "bool\ntrue", &pb.KnownTypes{Bool: &wpb.BoolValue{Value: true}}},
	{"StringValue", Unmarshaler{}, "str\nplush", &pb.KnownTypes{Str: &wpb.StringValue{Value: "plush"}}},
	{"StringValue containing escaped character", Unmarshaler{}, "str\na/b", &pb.KnownTypes{Str: &wpb.StringValue{Value: "a/b"}}},
	{"BytesValue", Unmarshaler{}, "bytes\nd293", &pb.KnownTypes{Bytes: &wpb.BytesValue{Value: []byte("wow")}}},


	// Ensure that `null` as a value ends up with a nil pointer instead of a [type]Value struct.
	{"null DoubleValue", Unmarshaler{}, "dbl\nnull", &pb.KnownTypes{Dbl: nil}},
	{"null FloatValue", Unmarshaler{}, "flt\nnull", &pb.KnownTypes{Flt: nil}},
	{"null Int64Value", Unmarshaler{}, "i64\nnull", &pb.KnownTypes{I64: nil}},
	{"null UInt64Value", Unmarshaler{}, "u64\nnull", &pb.KnownTypes{U64: nil}},
	{"null Int32Value", Unmarshaler{}, "i32\nnull", &pb.KnownTypes{I32: nil}},
	{"null UInt32Value", Unmarshaler{}, "u32\nnull", &pb.KnownTypes{U32: nil}},
	{"null BoolValue", Unmarshaler{}, "bool\nnull", &pb.KnownTypes{Bool: nil}},
	{"null StringValue", Unmarshaler{}, "str\nnull", &pb.KnownTypes{Str: nil}},
	{"null BytesValue", Unmarshaler{}, "bytes\nnull", &pb.KnownTypes{Bytes: nil}},

	{"required", Unmarshaler{}, "str\nhello", &pb.MsgWithRequired{Str: proto.String("hello")}},
	{"required bytes", Unmarshaler{}, "byts\n\"\"", &pb.MsgWithRequiredBytes{Byts: []byte{}}},
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
