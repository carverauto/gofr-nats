package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gofr.dev/pkg/gofr/datasource"
	"gofr.dev/pkg/gofr/datasource/pubsub"
)

//go:generate mockgen -destination=mock_jetstream.go -package=nats github.com/nats-io/nats.go/jetstream JetStream,Stream,Consumer,Msg,MessageBatch

const (
	ctxCloseTimeout = 5 * time.Second
)

type ConnectionManager struct {
	conn             ConnInterface
	jetStream        jetstream.JetStream
	config           *Config
	logger           pubsub.Logger
	natsConnector    NATSConnector
	jetStreamCreator JetStreamCreator
}

func (cm *ConnectionManager) JetStream() (jetstream.JetStream, error) {
	if cm.jetStream == nil {
		return nil, errJetStreamNotConfigured
	}

	return cm.jetStream, nil
}

// natsConnWrapper wraps a nats.Conn to implement the ConnInterface.
type natsConnWrapper struct {
	conn *nats.Conn
}

func (w *natsConnWrapper) Status() nats.Status {
	return w.conn.Status()
}

func (w *natsConnWrapper) Close() {
	w.conn.Close()
}

func (w *natsConnWrapper) NATSConn() *nats.Conn {
	return w.conn
}

func (w *natsConnWrapper) JetStream() (jetstream.JetStream, error) {
	return jetstream.New(w.conn)
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(
	cfg *Config,
	logger pubsub.Logger,
	natsConnector NATSConnector,
	jetStreamCreator JetStreamCreator) *ConnectionManager {
	// if logger is nil panic
	if logger == nil {
		panic("logger is required")
	}

	if natsConnector == nil {
		natsConnector = &DefaultNATSConnector{}
	}

	if jetStreamCreator == nil {
		jetStreamCreator = &DefaultJetStreamCreator{}
	}

	return &ConnectionManager{
		config:           cfg,
		logger:           logger,
		natsConnector:    natsConnector,
		jetStreamCreator: jetStreamCreator,
	}
}

// Connect establishes a connection to NATS and sets up JetStream.
func (cm *ConnectionManager) Connect(ctx context.Context) error {
	cm.logger.Logf("Connecting to NATS server at %v", cm.config.Server)

	opts := []nats.Option{nats.Name("GoFr NATS JetStreamClient")}

	if cm.config.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cm.config.CredsFile))
	}

	connInterface, err := cm.natsConnector.Connect(cm.config.Server, opts...)
	if err != nil {
		cm.logger.Errorf("failed to connect to NATS server at %v: %v", cm.config.Server, err)

		return err
	}

	js, err := cm.jetStreamCreator.New(connInterface)
	if err != nil {
		connInterface.Close()
		cm.logger.Errorf("failed to create JetStream context: %v", err)

		return err
	}

	cm.conn = connInterface
	cm.jetStream = js

	return nil
}

func (cm *ConnectionManager) Close(ctx context.Context) {
	_, cancel := context.WithTimeout(ctx, ctxCloseTimeout)
	defer cancel()

	if cm.conn != nil {
		cm.conn.Close()
	}
}

func (cm *ConnectionManager) Publish(ctx context.Context, subject string, message []byte, metrics Metrics) error {
	metrics.IncrementCounter(ctx, "app_pubsub_publish_total_count", "subject", subject)

	if err := cm.validateJetStream(subject); err != nil {
		return err
	}

	_, err := cm.jetStream.Publish(ctx, subject, message)
	if err != nil {
		cm.logger.Errorf("failed to publish message to NATS JetStream: %v", err)
		return err
	}

	metrics.IncrementCounter(ctx, "app_pubsub_publish_success_count", "subject", subject)

	return nil
}

func (cm *ConnectionManager) validateJetStream(subject string) error {
	if cm.jetStream == nil || subject == "" {
		err := errJetStreamNotConfigured
		cm.logger.Error(err.Error())

		return err
	}

	return nil
}

func (cm *ConnectionManager) Health() datasource.Health {
	if cm.conn == nil {
		return datasource.Health{
			Status: datasource.StatusDown,
		}
	}

	status := cm.conn.Status()
	if status == nats.CONNECTED {
		return datasource.Health{
			Status: datasource.StatusUp,
			Details: map[string]interface{}{
				"server": cm.config.Server,
			},
		}
	}

	return datasource.Health{
		Status: datasource.StatusDown,
		Details: map[string]interface{}{
			"server": cm.config.Server,
		},
	}
}
