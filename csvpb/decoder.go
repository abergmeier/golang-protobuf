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
	"bufio"
	"encoding/csv"
	"io"
)

// Decoder decodes single line
type Decoder struct {
	buffer *bufio.Reader
	reader *csv.Reader
	v []string
	err error
	reportedError bool
}

// NewDecoder creates a new Decoder. Internal state is implementation detail.
func NewDecoder(r io.Reader) *Decoder {

	br := bufio.NewReader(r)
	d := &Decoder{
		buffer: br,
		reader: csv.NewReader(br),
	}

	d.prefetch()
	return d
}

func (d *Decoder) prefetch() {
	next, _ := d.buffer.Peek(1)
	d.v, d.err = d.reader.Read()
	if len(next) == 0 {
		// There was nothing to read
		d.v = nil
		d.err = io.EOF
	}
}

// More returns whether there is another value to return
func (d *Decoder) More() bool {
	if d.err == nil {
		// We have a new value available
		return true
	}

	if d.err == io.EOF {
		return false
	}

	// Has the error been reported yet?
	return !d.reportedError
}

// Decode extracts a slice of strings from next line. Returns nil when
// nothing else to extract
func (d *Decoder) Decode() ([]string, error) {
	// Value and error are already prefetched
	if d.err != nil {
		d.reportedError = true
		if d.err == io.EOF {
			return d.v, nil
		}

		// Do not allow advancing beyond an error
		return nil, d.err
	}

	currentV, currentErr := d.v, d.err
	d.prefetch()
	return currentV, currentErr
}
