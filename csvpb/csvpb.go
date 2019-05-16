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

/*
Package csvpb provides unmarshaling between protocol buffers and RFC 4180.
*/
package csvpb

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"encoding/base64"

	stpb "github.com/golang/protobuf/ptypes/struct"
)

const (
	secondInNanos = int64(time.Second / time.Nanosecond)

	NONE = iota
	BOOL
	DOUBLE
	FLOAT
	INT32
	INT64
	STRING
	UINT32
	UINT64

)

// CSVPBUnmarshaler is implemented by protobuf messages that customize
// the way they are unmarshaled from CSV. Messages that implement this
// should also implement CSVPBMarshaler so that the custom format can be
// produced.
type CSVPBUnmarshaler interface {
	UnmarshalJSONPB(*Unmarshaler, []byte) error
}

type int32Slice []int32

var nonFinite = map[string]float64{
	`"NaN"`:       math.NaN(),
	`"Infinity"`:  math.Inf(1),
	`"-Infinity"`: math.Inf(-1),
}

// For sorting extensions ids to ensure stable output.
func (s int32Slice) Len() int           { return len(s) }
func (s int32Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s int32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type wkt interface {
	XXX_WellKnownType() string
}

// An InvalidUnmarshalError describes an invalid argument passed to Unmarshal.
// (The argument to Unmarshal must be a non-nil pointer.)
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "csv: Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "csv: Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "csv: Unmarshal(nil " + e.Type.String() + ")"
}

// Unmarshaler is a configurable object for converting from a CSV
// representation to a protocol buffer object.
type Unmarshaler struct {
	// Whether to allow messages to contain unknown fields, as opposed to
	// failing to unmarshal.
	AllowUnknownFields bool

	Header []string
}

// UnmarshalNext unmarshals the next protocol buffer from a CSV.
// This function is lenient and will decode any options permutations of the
// related Marshaler.
func (u *Unmarshaler) UnmarshalNext(dec *Decoder, pb proto.Message) error {
	var inputValue []string
	var err error
	if inputValue, err = dec.Decode(); err != nil {
		return err
	}
	if err := u.unmarshalRecord(reflect.ValueOf(pb).Elem(), inputValue, nil); err != nil {
		return err
	}
	return checkRequiredFields(pb)
}

// Unmarshal unmarshals a CSV object stream into a protocol
// buffer. This function is lenient and will decode any options
// permutations of the related Marshaler.
func (u *Unmarshaler) Unmarshal(r io.Reader, pb proto.Message) error {
	csvReader := csv.NewReader(r)
	if u.Header == nil {
		var err error
		u.Header, err = csvReader.Read()
		if err != nil {
			return err
		}
	}
	dec := NewDecoder(csvReader)
	return u.UnmarshalNext(dec, pb)
}

func (u *Unmarshaler) unmarshalList(value RawMessage) error {

	return nil
}

// UnmarshalNext unmarshals the next protocol buffer from a JSON object stream.
// This function is lenient and will decode any options permutations of the
// related Marshaler.
func UnmarshalNext(dec *Decoder, pb proto.Message) error {
	return new(Unmarshaler).UnmarshalNext(dec, pb)
}

// Unmarshal unmarshals a JSON object stream into a protocol
// buffer. This function is lenient and will decode any options
// permutations of the related Marshaler.
func Unmarshal(r io.Reader, pb proto.Message) error {
	return new(Unmarshaler).Unmarshal(r, pb)
}

// UnmarshalString will populate the fields of a protocol buffer based
// on a JSON string. This function is lenient and will decode any options
// permutations of the related Marshaler.
func UnmarshalString(str string, pb proto.Message) error {
	return new(Unmarshaler).Unmarshal(strings.NewReader(str), pb)
}

// unmarshalRecord converts/copies a record into the target.
// prop may be nil.
func (u *Unmarshaler) unmarshalRecord(target reflect.Value, inputRecord []string, prop *proto.Properties) error {
	targetType := target.Type()

	// Handle struct.
	if targetType.Kind() == reflect.Struct {
		csvFields := make(map[string]string)
		if err := u.csvUnmarshal(target, u.Header, inputRecord, &csvFields); err != nil {
			return err
		}

		consumeField := func(prop *proto.Properties) (string, bool) {
			// Be liberal in what names we accept; both orig_name and camelName are okay.
			fieldNames := acceptedJSONFieldNames(prop)

			vOrig, okOrig := csvFields[fieldNames.orig]
			vCamel, okCamel := csvFields[fieldNames.camel]
			if !okOrig && !okCamel {
				return "", false
			}
			// If, for some reason, both are present in the data, favour the camelName.
			var raw string
			if okOrig {
				raw = vOrig
				delete(csvFields, fieldNames.orig)
			}
			if okCamel {
				raw = vCamel
				delete(csvFields, fieldNames.camel)
			}
			return raw, true
		}

		sprops := proto.GetProperties(targetType)
		for i := 0; i < target.NumField(); i++ {
			ft := target.Type().Field(i)
			if strings.HasPrefix(ft.Name, "XXX_") {
				continue
			}

			valueForField, ok := consumeField(sprops.Prop[i])
			if !ok {
				continue
			}

			if err := u.unmarshalValue(target.Field(i), valueForField, sprops.Prop[i], NONE); err != nil {
				return err
			}

		}

		// Check for any oneof fields.
		if len(csvFields) > 0 {
			for _, oop := range sprops.OneofTypes {
				raw, ok := consumeField(oop.Prop)
				if !ok {
					continue
				}
				nv := reflect.New(oop.Type.Elem())
				target.Field(oop.Field).Set(nv)
				if err := u.unmarshalValue(nv.Elem().Field(0), raw, oop.Prop, NONE); err != nil {
					return err
				}
			}
		}

		// No support for proto2 extensions.

		if !u.AllowUnknownFields && len(csvFields) > 0 {
			// Pick any field to be the scapegoat.
			var f string
			for fname := range csvFields {
				f = fname
				break
			}
			return fmt.Errorf("unknown field %q in %v", f, targetType)
		}
		return nil
	}

	panic("FALLBACK NOT IMPLEMENTED")
}

func (u *Unmarshaler) csvUnmarshal(target reflect.Value, fieldNames []string, fields []string, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(v)}
	}

	// Dereference
	rv = rv.Elem()

	if rv.Kind() == reflect.Map {
		for i, fieldName := range fieldNames {
			fieldValue := fields[i]
			rv.SetMapIndex(reflect.ValueOf(fieldName), reflect.ValueOf(fieldValue))
		}
		return nil
	}

	return &InvalidUnmarshalError{reflect.TypeOf(v)}
}

// unmarshalValue converts/copies a value into the target.
// prop may be nil.
func (u *Unmarshaler) unmarshalValue(target reflect.Value, inputValue string, prop *proto.Properties, typeHint int) error {
	targetType := target.Type()

	// Allocate memory for pointer fields.
	if targetType.Kind() == reflect.Ptr {
		// If input value is "null" and target is a pointer type, then the field should be treated as not set
		// UNLESS the target is structpb.Value, in which case it should be set to structpb.NullValue.
		_, isCSVPBUnmarshaler := target.Interface().(CSVPBUnmarshaler)
		if string(inputValue) == "null" && targetType != reflect.TypeOf(&stpb.Value{}) && !isCSVPBUnmarshaler {
			return nil
		}
		target.Set(reflect.New(targetType.Elem()))

		return u.unmarshalValue(target.Elem(), inputValue, prop, NONE)
	}

	// Handle well-known types that are not pointers.
	if w, ok := target.Addr().Interface().(wkt); ok {
		switch w.XXX_WellKnownType() {
		case "DoubleValue":
			return u.unmarshalValue(target.Field(0), inputValue, prop, DOUBLE)
		case "FloatValue":
			return u.unmarshalValue(target.Field(0), inputValue, prop, FLOAT)
		case "Int64Value":
			return u.unmarshalValue(target.Field(0), inputValue, prop, INT64)
		case "UInt64Value":
			return u.unmarshalValue(target.Field(0), inputValue, prop, UINT64)
		case "Int32Value":
			return u.unmarshalValue(target.Field(0), inputValue, prop, INT32)
		case "UInt32Value":
			return u.unmarshalValue(target.Field(0), inputValue, prop, UINT32)
		case "BoolValue":
			return u.unmarshalValue(target.Field(0), inputValue, prop, BOOL)
		case "StringValue":
			return u.unmarshalValue(target.Field(0), inputValue, prop, STRING)
		case "BytesValue":
			return u.unmarshalValue(target.Field(0), inputValue, prop, NONE)
		case "Any":
			return errors.New("Cannot determine type of Any")
		case "Duration":
			// TODO: Possibly unquote necessary
			unq := string(inputValue)

			d, err := time.ParseDuration(unq)
			if err != nil {
				return fmt.Errorf("bad Duration: %v", err)
			}

			ns := d.Nanoseconds()
			s := ns / 1e9
			ns %= 1e9
			target.Field(0).SetInt(s)
			target.Field(1).SetInt(ns)
			return nil
		case "Timestamp":
			// TODO: Possibly unquote necessary
			unq := string(inputValue)

			t, err := time.Parse(time.RFC3339Nano, unq)
			if err != nil {
				return fmt.Errorf("bad Timestamp: %v", err)
			}

			target.Field(0).SetInt(t.Unix())
			target.Field(1).SetInt(int64(t.Nanosecond()))
			return nil
		case "ListValue":
			var s []string
			if inputValue == "" {
				s = []string{}
			} else {
				csvReader := csv.NewReader(strings.NewReader(inputValue))
				var err error
				s, err = csvReader.Read()
				if err != nil {
					return fmt.Errorf("bad ListValue: %v", err)
				}
			}

			target.Field(0).Set(reflect.ValueOf(make([]*stpb.Value, len(s))))
			for i, sv := range s {
				if err := u.unmarshalValue(target.Field(0).Index(i), sv, prop, NONE); err != nil {
					return err
				}
			}
			return nil
		case "Value":
			ivStr := string(inputValue)
			if ivStr == "" {
				target.Field(0).Set(reflect.ValueOf(&stpb.Value_NullValue{}))
				return nil
			}

			if strings.Contains(ivStr, ".") {
				if v, err := strconv.ParseFloat(ivStr, 0); err == nil {
					target.Field(0).Set(reflect.ValueOf(&stpb.Value_NumberValue{v}))
					return nil
				}
			}

			if ivStr != "1" && ivStr != "t" && ivStr != "T" && ivStr != "0" && ivStr != "f" && ivStr != "F" {
				if v, err := strconv.ParseBool(ivStr); err == nil {
					target.Field(0).Set(reflect.ValueOf(&stpb.Value_BoolValue{v}))
					return nil
				}
			}

			if v, err := strconv.ParseFloat(ivStr, 0); err == nil {
				target.Field(0).Set(reflect.ValueOf(&stpb.Value_NumberValue{v}))
				return nil
			}
			
			// There is no good way to detect signedness so default to plain
			// string for anything else
			target.Field(0).Set(reflect.ValueOf(&stpb.Value_StringValue{ivStr}))
			return nil
		}
	}

	// Handle nested messages.
	if targetType.Kind() == reflect.Struct {
		return errors.New("Nested messages not supported yet")
	}

	// Handle arrays
	if targetType.Kind() == reflect.Slice {
		// Handle encoded bytes
		if targetType.Elem().Kind() == reflect.Uint8 {
			decoded, err := base64.StdEncoding.DecodeString(inputValue)
			if err != nil {
				return err
			}
			target.SetBytes(decoded)
			return nil
		}

		csvReader := csv.NewReader(strings.NewReader(inputValue))
		slc, err := csvReader.Read()
		if err != nil {
			return err
		}

		if slc != nil {
			l := len(slc)
			target.Set(reflect.MakeSlice(targetType, l, l))
			for i := 0; i < l; i++ {
				if err := u.unmarshalValue(target.Index(i), slc[i], prop, NONE); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Does not handle embedded maps

	// Handle enums, which have an underlying type of int32,
	// and may appear as strings.
	// The case of an enum appearing as a number is handled
	// at the bottom of this function.
	if prop != nil && prop.Enum != "" {
		vmap := proto.EnumValueMap(prop.Enum)
		inputValue = strings.TrimSpace(inputValue)
		s := inputValue
		n, ok := vmap[s]
		if !ok {
			// Check whether input is a number and thus we handle it later
			_, err := strconv.ParseUint(s, 10, 32)
			if err != nil {
				return fmt.Errorf("unknown value %q for enum %s", s, prop.Enum)
			}
			ok = false
		}
		if ok { // Only process string
			if targetType.Kind() != reflect.Int32 {
				return fmt.Errorf("invalid target %q for enum %s", targetType.Kind(), prop.Enum)
			}
			target.SetInt(int64(n))
			return nil
		}
	}

	isBool := targetType.Kind() == reflect.Bool
	if isBool {
		if strings.HasPrefix(string(inputValue), `"`) {
			inputValue = inputValue[1 : len(inputValue)-1]
		}

		lowerValue := strings.ToLower(inputValue)
		boolValue, err := strconv.ParseBool(lowerValue)
		if err != nil {
			return err
		}
		target.SetBool(boolValue)
		return nil
	}

	// Non-finite numbers can be encoded as strings.
	isFloat := targetType.Kind() == reflect.Float32 || targetType.Kind() == reflect.Float64
	if isFloat {
		if num, ok := nonFinite[string(inputValue)]; ok {
			target.SetFloat(num)
			return nil
		}
	}

	// integers & floats can be encoded as strings. In this case we drop
	// the quotes and proceed as normal.
	isNum := targetType.Kind() == reflect.Int64 || targetType.Kind() == reflect.Uint64 ||
		targetType.Kind() == reflect.Int32 || targetType.Kind() == reflect.Uint32 ||
		targetType.Kind() == reflect.Float32 || targetType.Kind() == reflect.Float64
	if isNum && strings.HasPrefix(string(inputValue), `"`) {
		inputValue = inputValue[1 : len(inputValue)-1]
	}

	switch targetType.Kind() {
	case reflect.Float32:
		floatValue, err := strconv.ParseFloat(inputValue, 32)
		if err != nil {
			return err
		}
		target.SetFloat(floatValue)
		return nil
	case reflect.Float64:
		floatValue, err := strconv.ParseFloat(inputValue, 64)
		if err != nil {
			return err
		}
		target.SetFloat(floatValue)
		return nil
	case reflect.Int32:
		intValue, err := strconv.ParseInt(inputValue, 10, 32)
		if err != nil {
			return err
		}
		target.SetInt(intValue)
		return nil
	case reflect.Int64:
		intValue, err := strconv.ParseInt(inputValue, 10, 64)
		if err != nil {
			return err
		}
		target.SetInt(intValue)
		return nil
	case reflect.Uint32:
		uintValue, err := strconv.ParseUint(inputValue, 10, 32)
		if err != nil {
			return err
		}
		target.SetUint(uintValue)
		return nil
	case reflect.Uint64:
		uintValue, err := strconv.ParseUint(inputValue, 10, 64)
		if err != nil {
			return err
		}
		target.SetUint(uintValue)
		return nil
	case reflect.String:
		target.SetString(inputValue)
		return nil
	}

	return errors.New("Not handled yet")
}

// jsonProperties returns parsed proto.Properties for the field and corrects JSONName attribute.
func jsonProperties(f reflect.StructField, origName bool) *proto.Properties {
	var prop proto.Properties
	prop.Init(f.Type, f.Name, f.Tag.Get("protobuf"), &f)
	if origName || prop.JSONName == "" {
		prop.JSONName = prop.OrigName
	}
	return &prop
}

type fieldNames struct {
	orig, camel string
}

func acceptedJSONFieldNames(prop *proto.Properties) fieldNames {
	opts := fieldNames{orig: prop.OrigName, camel: prop.OrigName}
	if prop.JSONName != "" {
		opts.camel = prop.JSONName
	}
	return opts
}

// Writer wrapper inspired by https://blog.golang.org/errors-are-values
type errWriter struct {
	writer io.Writer
	err    error
}

func (w *errWriter) write(str string) {
	if w.err != nil {
		return
	}
	_, w.err = w.writer.Write([]byte(str))
}

// Map fields may have key types of non-float scalars, strings and enums.
// The easiest way to sort them in some deterministic order is to use fmt.
// If this turns out to be inefficient we can always consider other options,
// such as doing a Schwartzian transform.
//
// Numeric keys are sorted in numeric order per
// https://developers.google.com/protocol-buffers/docs/proto#maps.
type mapKeys []reflect.Value

func (s mapKeys) Len() int      { return len(s) }
func (s mapKeys) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s mapKeys) Less(i, j int) bool {
	if k := s[i].Kind(); k == s[j].Kind() {
		switch k {
		case reflect.String:
			return s[i].String() < s[j].String()
		case reflect.Int32, reflect.Int64:
			return s[i].Int() < s[j].Int()
		case reflect.Uint32, reflect.Uint64:
			return s[i].Uint() < s[j].Uint()
		}
	}
	return fmt.Sprint(s[i].Interface()) < fmt.Sprint(s[j].Interface())
}

// checkRequiredFields returns an error if any required field in the given proto message is not set.
// This function is used by both Marshal and Unmarshal.  While required fields only exist in a
// proto2 message, a proto3 message can contain proto2 message(s).
func checkRequiredFields(pb proto.Message) error {
	// Most well-known type messages do not contain required fields.  The "Any" type may contain
	// a message that has required fields.
	//
	// When an Any message is being marshaled, the code will invoked proto.Unmarshal on Any.Value
	// field in order to transform that into JSON, and that should have returned an error if a
	// required field is not set in the embedded message.
	//
	// When an Any message is being unmarshaled, the code will have invoked proto.Marshal on the
	// embedded message to store the serialized message in Any.Value field, and that should have
	// returned an error if a required field is not set.
	if _, ok := pb.(wkt); ok {
		return nil
	}

	v := reflect.ValueOf(pb)
	// Skip message if it is not a struct pointer.
	if v.Kind() != reflect.Ptr {
		return nil
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return nil
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		sfield := v.Type().Field(i)

		if sfield.PkgPath != "" {
			// blank PkgPath means the field is exported; skip if not exported
			continue
		}

		if strings.HasPrefix(sfield.Name, "XXX_") {
			continue
		}

		// Oneof field is an interface implemented by wrapper structs containing the actual oneof
		// field, i.e. an interface containing &T{real_value}.
		if sfield.Tag.Get("protobuf_oneof") != "" {
			if field.Kind() != reflect.Interface {
				continue
			}
			v := field.Elem()
			if v.Kind() != reflect.Ptr || v.IsNil() {
				continue
			}
			v = v.Elem()
			if v.Kind() != reflect.Struct || v.NumField() < 1 {
				continue
			}
			field = v.Field(0)
			sfield = v.Type().Field(0)
		}

		protoTag := sfield.Tag.Get("protobuf")
		if protoTag == "" {
			continue
		}
		var prop proto.Properties
		prop.Init(sfield.Type, sfield.Name, protoTag, &sfield)

		switch field.Kind() {
		case reflect.Map:
			if field.IsNil() {
				continue
			}
			// Check each map value.
			keys := field.MapKeys()
			for _, k := range keys {
				v := field.MapIndex(k)
				if err := checkRequiredFieldsInValue(v); err != nil {
					return err
				}
			}
		case reflect.Slice:
			// Handle non-repeated type, e.g. bytes.
			if !prop.Repeated {
				if prop.Required && field.IsNil() {
					return fmt.Errorf("required field %q is not set", prop.Name)
				}
				continue
			}

			// Handle repeated type.
			if field.IsNil() {
				continue
			}
			// Check each slice item.
			for i := 0; i < field.Len(); i++ {
				v := field.Index(i)
				if err := checkRequiredFieldsInValue(v); err != nil {
					return err
				}
			}
		case reflect.Ptr:
			if field.IsNil() {
				if prop.Required {
					return fmt.Errorf("required field %q is not set", prop.Name)
				}
				continue
			}
			if err := checkRequiredFieldsInValue(field); err != nil {
				return err
			}
		}
	}

	// Handle proto2 extensions.
	for _, ext := range proto.RegisteredExtensions(pb) {
		if !proto.HasExtension(pb, ext) {
			continue
		}
		ep, err := proto.GetExtension(pb, ext)
		if err != nil {
			return err
		}
		err = checkRequiredFieldsInValue(reflect.ValueOf(ep))
		if err != nil {
			return err
		}
	}

	return nil
}

func checkRequiredFieldsInValue(v reflect.Value) error {
	if pm, ok := v.Interface().(proto.Message); ok {
		return checkRequiredFields(pm)
	}
	return nil
}
