package support

import (
	"bufio"
	"io"
)

func EnsureBufferedReader(r io.Reader) *bufio.Reader {
	if br, ok := r.(*bufio.Reader); ok {
		return br
	} else {
		return bufio.NewReader(r)
	}
}

func EnsureBufferedWriter(w io.Writer) *bufio.Writer {
	if br, ok := w.(*bufio.Writer); ok {
		return br
	} else {
		return bufio.NewWriter(w)
	}
}
