package messaging

import (
	"time"

	"auradb-pipeline/internal/config"

	"github.com/nats-io/nats.go"
)

type NatsRepository struct {
	conn *nats.Conn
}

func NewNatsRepository(cfg config.Config) (*NatsRepository, error) {
	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		return nil, err
	}

	return &NatsRepository{
		conn: nc,
	}, nil
}

func (r *NatsRepository) Publish(subject string, data []byte) error {
	if err := r.conn.Publish(subject, data); err != nil {
		return err
	}
	return r.conn.FlushTimeout(5 * time.Second)
}
