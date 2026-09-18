package jobs

import "errors"

// ErrExists is returned when creating a job with a known id.
var ErrExists = errors.New("job already exists")
