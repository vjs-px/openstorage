package volume

import (
	"io"

	"github.com/libopenstorage/openstorage/api"
)

// DefaultGraphDriver is a default (null) graph driver implementation.  This can be
// used by drivers that do not want to (or care about) implementing the
// graph driver interface.
type DefaultGraphDriver struct {
}

func (d *DefaultGraphDriver) GraphDriverCreate(id, parent string) error {
	return ErrNotSupported
}

func (d *DefaultGraphDriver) GraphDriverRemove(id string) error {
	return ErrNotSupported
}

func (d *DefaultGraphDriver) GraphDriverGet(id, mountLabel string) (string, error) {
	return "", ErrNotSupported
}

func (d *DefaultGraphDriver) GraphDriverRelease(id string) error {
	return ErrNotSupported
}

func (d *DefaultGraphDriver) GraphDriverExists(id string) bool {
	return false
}

func (d *DefaultGraphDriver) GraphDriverDiff(id, parent string) io.Writer {
	return nil
}

func (d *DefaultGraphDriver) GraphDriverChanges(id, parent string) ([]api.GraphDriverChanges, error) {
	changes := make([]api.GraphDriverChanges, 0)
	return changes, ErrNotSupported
}

func (d *DefaultGraphDriver) GraphDriverApplyDiff(id, parent string, diff io.Reader) (int, error) {
	return 0, ErrNotSupported
}

func (d *DefaultGraphDriver) GraphDriverDiffSize(id, parent string) (int, error) {
	return 0, ErrNotSupported
}
