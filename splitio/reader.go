/*
Package splitio provides tools to splitting io (e.g. Readers) so it can easily
been worked with when using Protobuf.
*/
package splitio

import (
	"bufio"
	"io"
	"sync"
)

func min(lhs int, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}

type lhsReader struct {
	br   *bufio.Reader
	wg   *sync.WaitGroup
	done bool
	sep  byte
}

func findByte(s []byte, sep byte) int {
	for i, v := range s {
		if v == sep {
			return i
		}
	}

	return -1
}

func (r *lhsReader) Read(p []byte) (n int, err error) {
	if r.done {
		return 0, io.EOF
	}

	if len(p) == 0 {
		return 0, nil
	}

	bufLen := min(len(p), 1024)
	array, peekErr := r.br.Peek(bufLen)
	if peekErr != nil && peekErr != io.EOF {
		return 0, peekErr
	}

	i := findByte(array, r.sep)
	if i == -1 {
		return r.br.Read(p)
	}

	// Read until sep
	p = p[:i]
	n, err = r.br.Read(p)
	if err != nil && err != io.EOF {
		return n, err
	}

	if err != io.EOF {
		_, err := r.br.ReadByte()
		if err != nil && err != io.EOF {
			return n, err
		}
	}

	r.done = true
	// Signal other reader may start
	r.wg.Done()
	return n, io.EOF
}

type rhsReader struct {
	br *bufio.Reader
	wg *sync.WaitGroup
}

func (r *rhsReader) Read(p []byte) (n int, err error) {
	r.wg.Wait()
	return r.br.Read(p)
}

// NewReadersSequential splits the input reader by a seperator.
// Returns a first Reader for reading everything until first occurence of
// said separator. Also a second Reader for everything after first occurence
// of said separator.
// Second Reader will only start once first Reader reached EOF.
func NewReadersSequential(r io.Reader, sep byte) (io.Reader, io.Reader) {
	br := bufio.NewReader(r)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return &lhsReader{
		br:  br,
		wg:  wg,
		sep: sep,
	}, &rhsReader{
		br: br,
		wg: wg,
	}
}
