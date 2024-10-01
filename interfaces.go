package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gofr.dev/pkg/gofr/datasource"
)

//go:generate mockgen -destination=mock_client.go -package=nats -source=./interfaces.go Client,Subscription,ConnInterface

// ConnInterface represents the main Client connection.
type ConnInterface interface {
	Status() nats.Status
	Close()
	NatsConn() *nats.Conn
}

// NATSConnector represents the main Client connection.
type NATSConnector interface {
	Connect(string, ...nats.Option) (ConnInterface, error)
}

// JetStreamCreator represents the main Client JetStream Client.
type JetStreamCreator interface {
	New(*nats.Conn) (jetstream.JetStream, error)
}

// JetStreamClient represents the main Client JetStream Client.
type JetStreamClient interface {
	Publish(ctx context.Context, subject string, message []byte) error
	Subscribe(ctx context.Context, subject string, handler messageHandler) error
	Close(ctx context.Context) error
	DeleteStream(ctx context.Context, name string) error
	CreateStream(ctx context.Context, cfg StreamConfig) error
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
	Health() datasource.Health
}
