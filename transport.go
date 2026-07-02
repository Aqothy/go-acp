package acp

import (
	"errors"
	"io"
	"os"
)

type duplex struct {
	r io.Reader
	w io.Writer
}

func (d duplex) Read(p []byte) (int, error)  { return d.r.Read(p) }
func (d duplex) Write(p []byte) (int, error) { return d.w.Write(p) }

func (d duplex) Close() error {
	var errs []error
	if c, ok := d.w.(io.Closer); ok {
		errs = append(errs, c.Close())
	}
	if c, ok := d.r.(io.Closer); ok {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}

// Combine joins a reader and a writer into the io.ReadWriteCloser expected by
// AgentApp.Connect and ClientApp.Connect. Close closes whichever halves
// implement io.Closer.
func Combine(r io.Reader, w io.Writer) io.ReadWriteCloser { return duplex{r: r, w: w} }

// Stdio is the standard agent transport: JSON-RPC read from stdin, written to stdout.
func Stdio() io.ReadWriteCloser { return Combine(os.Stdin, os.Stdout) }
