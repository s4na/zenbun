// Package partupload buffers at most one upload part and keeps the final
// part until the producer has reported success.
package partupload

import (
	"context"
	"errors"
	"io"
	"time"
)

const Size = 8 << 20

type writer struct {
	buf  []byte
	send func([]byte) error
	err  error
}

func (w *writer) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 && w.err == nil {
		if len(w.buf) == Size {
			w.err = w.send(w.buf)
			if w.err != nil {
				break
			}
			w.buf = w.buf[:0]
		}
		count := min(len(p), Size-len(w.buf))
		w.buf = append(w.buf, p[:count]...)
		n += count
		p = p[count:]
	}
	return n, w.err
}

// Run calls single for objects up to Size. Larger objects call part for
// each non-final chunk, then finish. Abort must tolerate an unstarted session.
func Run(ctx context.Context, produce func(io.Writer) error, single, part, finish func([]byte) error, abort func(context.Context) error) (err error) {
	multipart := false
	w := writer{send: func(p []byte) error { multipart = true; return part(p) }}
	defer func() {
		if multipart {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if e := abort(cleanup); e != nil {
				err = errors.Join(err, e)
			}
		}
	}()
	if err = produce(&w); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if !multipart {
		return single(w.buf)
	}
	if err = finish(w.buf); err == nil {
		multipart = false
	}
	return err
}
