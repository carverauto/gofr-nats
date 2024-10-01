package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
	"gofr.dev/pkg/gofr/datasource/pubsub"
)

//go:generate mockgen -destination=mock_jetstream.go -package=nats github.com/nats-io/nats.go/jetstream JetStream,Stream,Consumer,Msg,MessageBatch

// Config defines the Client Client configuration.
type Config struct {
	Server      string
	CredsFile   string
	Stream      StreamConfig
	Consumer    string
	MaxWait     time.Duration
	MaxPullWait int
	BatchSize   int
}

// StreamConfig holds stream settings for Client JetStream.
type StreamConfig struct {
	Stream     string
	Subjects   []string
	MaxDeliver int
	MaxWait    time.Duration
}

// subscription holds subscription information for Client JetStream.
type subscription struct {
	cancel context.CancelFunc
}

type messageHandler func(context.Context, jetstream.Msg) error

// Client represents a Client for Client JetStream operations.
type Client struct {
	Conn          ConnInterface
	JetStream     jetstream.JetStream
	Logger        pubsub.Logger
	Config        *Config
	Metrics       Metrics
	Subscriptions map[string]*subscription
	subMu         sync.Mutex
	Tracer        trace.Tracer
	messageBuffer chan *pubsub.Message
	bufferSize    int
}

// CreateTopic creates a new topic (stream) in Client JetStream.
func (n *Client) CreateTopic(ctx context.Context, name string) error {
	return n.CreateStream(ctx, StreamConfig{
		Stream:   name,
		Subjects: []string{name},
	})
}

// DeleteTopic deletes a topic (stream) in Client JetStream.
func (n *Client) DeleteTopic(ctx context.Context, name string) error {
	n.Logger.Debugf("deleting topic (stream) %s", name)

	err := n.JetStream.DeleteStream(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			n.Logger.Debugf("stream %s not found, considering delete successful", name)

			return nil // If the stream doesn't exist, we consider it a success
		}

		n.Logger.Errorf("failed to delete stream (topic) %s: %v", name, err)

		return err
	}

	n.Logger.Debugf("successfully deleted topic (stream) %s", name)

	return nil
}

// natsConnWrapper wraps a nats.Conn to implement the ConnInterface.
type natsConnWrapper struct {
	*nats.Conn
}

func (w *natsConnWrapper) Status() nats.Status {
	return w.Conn.Status()
}

func (w *natsConnWrapper) Close() {
	w.Conn.Close()
}

func (w *natsConnWrapper) NatsConn() *nats.Conn {
	return w.Conn
}

// New creates a new Client.
func New(cfg *Config) *PubSubWrapper {
	if cfg == nil {
		cfg = &Config{}
	}

	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100 // Default batch size
	}

	client := &Client{
		Config:        cfg,
		Subscriptions: make(map[string]*subscription),
		messageBuffer: make(chan *pubsub.Message, cfg.BatchSize),
		bufferSize:    cfg.BatchSize,
	}

	return &PubSubWrapper{Client: client}
}

// UseLogger sets the logger for the NATS client.
func (n *Client) UseLogger(logger any) {
	if l, ok := logger.(pubsub.Logger); ok {
		n.Logger = l
	}
}

// UseTracer sets the tracer for the NATS client.
func (n *Client) UseTracer(tracer any) {
	if t, ok := tracer.(trace.Tracer); ok {
		n.Tracer = t
	}
}

// UseMetrics sets the metrics for the NATS client.
func (n *Client) UseMetrics(metrics any) {
	if m, ok := metrics.(Metrics); ok {
		n.Metrics = m
	}
}

// Connect establishes a connection to NATS and sets up JetStream.
func (n *Client) Connect() {
	if err := n.validateAndPrepare(); err != nil {
		return
	}

	nc, err := n.createNATSConnection()
	if err != nil {
		return
	}

	js, err := n.createJetStreamContext(nc)
	if err != nil {
		nc.Close()
		return
	}

	n.Conn = &natsConnWrapper{nc}
	n.JetStream = js

	n.logSuccessfulConnection()
}

func (n *Client) validateAndPrepare() error {
	if n.Config == nil {
		n.Logger.Errorf("NATS configuration is nil")
		return errNATSConnNil
	}

	if err := ValidateConfigs(n.Config); err != nil {
		n.Logger.Errorf("could not initialize NATS JetStream: %v", err)
		return err
	}

	return nil
}

func (n *Client) createNATSConnection() (*nats.Conn, error) {
	opts := []nats.Option{nats.Name("GoFr NATS JetStreamClient")}
	if n.Config.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(n.Config.CredsFile))
	}

	nc, err := nats.Connect(n.Config.Server, opts...)
	if err != nil {
		n.Logger.Errorf("failed to connect to NATS server at %v: %v", n.Config.Server, err)
		return nil, err
	}

	return nc, nil
}

func (n *Client) createJetStreamContext(nc *nats.Conn) (jetstream.JetStream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		n.Logger.Errorf("failed to create JetStream context: %v", err)
		return nil, err
	}

	return js, nil
}

func (n *Client) logSuccessfulConnection() {
	if n.Logger != nil {
		n.Logger.Logf("connected to NATS server '%s'", n.Config.Server)
	}
}

// Publish publishes a message to a topic.
func (n *Client) Publish(ctx context.Context, subject string, message []byte) error {
	n.Metrics.IncrementCounter(ctx, "app_pubsub_publish_total_count", "subject", subject)

	if n.JetStream == nil || subject == "" {
		err := errJetStreamNotConfigured
		n.Logger.Error(err.Error())

		return err
	}

	_, err := n.JetStream.Publish(ctx, subject, message)
	if err != nil {
		n.Logger.Errorf("failed to publish message to Client JetStream: %v", err)

		return err
	}

	n.Metrics.IncrementCounter(ctx, "app_pubsub_publish_success_count", "subject", subject)

	return nil
}

func (n *Client) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	n.Metrics.IncrementCounter(ctx, "app_pubsub_subscribe_total_count", "topic", topic)

	if err := n.validateSubscribePrerequisites(); err != nil {
		return nil, err
	}

	cons, err := n.createOrUpdateConsumer(ctx, topic)
	if err != nil {
		return nil, err
	}

	return n.fetchAndProcessMessage(ctx, cons, topic)
}

func (n *Client) validateSubscribePrerequisites() error {
	if n.JetStream == nil {
		return errJetStreamNotConfigured
	}

	if n.Config.Consumer == "" {
		return errConsumerNotProvided
	}

	return nil
}

func (n *Client) createOrUpdateConsumer(ctx context.Context, topic string) (jetstream.Consumer, error) {
	consumerName := fmt.Sprintf("%s_%s", n.Config.Consumer, strings.ReplaceAll(topic, ".", "_"))
	cons, err := n.JetStream.CreateOrUpdateConsumer(ctx, n.Config.Stream.Stream, jetstream.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: topic,
		MaxDeliver:    n.Config.Stream.MaxDeliver,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckWait:       30 * time.Second,
	})

	if err != nil {
		n.Logger.Errorf("failed to create or update consumer: %v", err)

		return nil, err
	}

	return cons, nil
}

func (n *Client) fetchAndProcessMessage(ctx context.Context, cons jetstream.Consumer, topic string) (*pubsub.Message, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, n.Config.MaxWait)
	defer cancel()

	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(n.Config.MaxWait))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errTimeoutWaitingForMsg
		}

		return nil, n.handleFetchError(err, topic)
	}

	select {
	case msg := <-msgs.Messages():
		if msg == nil {
			return nil, errNoMsgReceivedForTopic
		}

		return n.handleReceivedMessage(msg, msgs, topic)
	case <-fetchCtx.Done():
		return nil, errTimeoutWaitingForMsg
	}
}

func (n *Client) handleFetchError(err error, topic string) error {
	if errors.Is(err, context.DeadlineExceeded) {
		n.Logger.Debugf("Timeout waiting for message on topic %s", topic)

		return errTimeoutWaitingForMsg
	}

	n.Logger.Errorf("Error fetching messages for topic %s: %v", topic, err)

	return errFetchMsgError
}

func (n *Client) handleReceivedMessage(msg jetstream.Msg, msgs jetstream.MessageBatch, topic string) (*pubsub.Message, error) {
	if msg == nil {
		n.Logger.Debugf("No messages received for topic %s", topic)

		return nil, errNoMsgReceivedForTopic
	}

	if fetchErr := msgs.Error(); fetchErr != nil {
		n.Logger.Errorf("Error in message batch for topic %s: %v", topic, fetchErr)

		return nil, fetchErr
	}

	n.Metrics.IncrementCounter(context.Background(), "app_pubsub_subscribe_success_count", "topic", topic)

	return &pubsub.Message{
		Topic:     topic,
		Value:     msg.Data(),
		MetaData:  msg.Headers(),
		Committer: &natsCommitter{msg: msg},
	}, nil
}

// HandleMessage handles a message from a consumer.
func (n *Client) HandleMessage(ctx context.Context, msg jetstream.Msg, handler messageHandler) error {
	if err := handler(ctx, msg); err != nil {
		n.Logger.Errorf("error handling message: %v", err)

		return n.NakMessage(msg)
	}

	return nil
}

// NakMessage naks a message from a consumer.
func (n *Client) NakMessage(msg jetstream.Msg) error {
	if err := msg.Nak(); err != nil {
		n.Logger.Errorf("failed to NAK message: %v", err)

		return err
	}

	return nil
}

// HandleFetchError handles fetch errors.
func (n *Client) HandleFetchError(err error) {
	n.Logger.Errorf("failed to fetch messages: %v", err)
	time.Sleep(time.Second) // Backoff on error
}

// Close closes the Client Client.
func (n *Client) Close() error {
	n.subMu.Lock()
	for _, sub := range n.Subscriptions {
		sub.cancel()
	}

	n.Subscriptions = make(map[string]*subscription)
	n.subMu.Unlock()

	if n.Conn != nil {
		n.Conn.Close()
	}

	return nil
}

// DeleteStream deletes a stream in Client JetStream.
func (n *Client) DeleteStream(ctx context.Context, name string) error {
	err := n.JetStream.DeleteStream(ctx, name)
	if err != nil {
		n.Logger.Errorf("failed to delete stream: %v", err)

		return err
	}

	return nil
}

// CreateStream creates a stream in Client JetStream.
func (n *Client) CreateStream(ctx context.Context, cfg StreamConfig) error {
	n.Logger.Debugf("creating stream %s", cfg.Stream)
	jsCfg := jetstream.StreamConfig{
		Name:     cfg.Stream,
		Subjects: cfg.Subjects,
	}

	_, err := n.JetStream.CreateStream(ctx, jsCfg)
	if err != nil {
		n.Logger.Errorf("failed to create stream: %v", err)

		return err
	}

	return nil
}

// CreateOrUpdateStream creates or updates a stream in Client JetStream.
func (n *Client) CreateOrUpdateStream(ctx context.Context, cfg *jetstream.StreamConfig) (jetstream.Stream, error) {
	n.Logger.Debugf("creating or updating stream %s", cfg.Name)

	stream, err := n.JetStream.CreateOrUpdateStream(ctx, *cfg)
	if err != nil {
		n.Logger.Errorf("failed to create or update stream: %v", err)

		return nil, err
	}

	return stream, nil
}

// ValidateConfigs validates the configuration for Client JetStream.
func ValidateConfigs(conf *Config) error {
	err := error(nil)

	if conf.Server == "" {
		err = errServerNotProvided
	}

	// check if subjects are provided
	if err == nil && len(conf.Stream.Subjects) == 0 {
		err = errSubjectsNotProvided
	}

	return err
}
