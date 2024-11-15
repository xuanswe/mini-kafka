package encoders

import (
	"github.com/pkg/errors"
	"github.com/xuanswe/mini-kafka/internal/models"
	"github.com/xuanswe/mini-kafka/internal/support"
	"io"
	"strings"
)

func ReadRequest(r io.Reader) (*models.Request, error) {
	if r == nil {
		return nil, errors.Errorf("nil reader")
	}

	reader := support.EnsureBufferedReader(r)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, errors.Wrap(err, "failed to read line")
	}

	return &models.Request{Data: strings.TrimSpace(line)}, nil
}
