package rabbitmq

import (
	"context"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

// defined in rabbitmq client package.
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

func isVaildExchange(name string) bool {
	switch name {
	case ExchangeDirect, ExchangeFanout, ExchangeTopic, ExchangeHeaders:
		return true
	default:
		return false
	}
}

// Option for queue system
type Option func(*options)

// AMQP 0-9-1 Model Explained
// ref: https://www.rabbitmq.com/tutorials/amqp-concepts.html
type options struct {
	runFunc func(context.Context, core.QueuedMessage) error
	logger  queue.Logger
	addr    string
	queue   string
	tag     string
	// Durable AMQP exchange name
	exchangeName string
	//  Exchange Types: Direct, Fanout, Topic and Headers
	exchangeType string
	autoAck      bool
	// AMQP routing key
	routingKey string
}

// WithAddr setup the URI
func WithAddr(addr string) Option {
	return func(w *options) {
		w.addr = addr
	}
}

// WithExchangeName setup the Exchange name
// Exchanges are AMQP 0-9-1 entities where messages are sent to.
// Exchanges take a message and route it into zero or more queues.
func WithExchangeName(val string) Option {
	return func(w *options) {
		w.exchangeName = val
	}
}

// WithExchangeType setup the Exchange type
// The routing algorithm used depends on the exchange type and rules called bindings.
// AMQP 0-9-1 brokers provide four exchange types:
// Direct exchange	(Empty string) and amq.direct
// Fanout exchange	amq.fanout
// Topic exchange	amq.topic
// Headers exchange	amq.match (and amq.headers in RabbitMQ)
func WithExchangeType(val string) Option {
	return func(w *options) {
		w.exchangeType = val
	}
}

// WithRoutingKey setup  AMQP routing key
func WithRoutingKey(val string) Option {
	return func(w *options) {
		w.routingKey = val
	}
}

// WithAddr setup the tag
func WithTag(val string) Option {
	return func(w *options) {
		w.tag = val
	}
}

// WithAutoAck enable message auto-ack
func WithAutoAck(val bool) Option {
	return func(w *options) {
		w.autoAck = val
	}
}

// WithQueue setup the queue name
func WithQueue(val string) Option {
	return func(w *options) {
		w.queue = val
	}
}

// WithRunFunc setup the run func of queue
func WithRunFunc(fn func(context.Context, core.QueuedMessage) error) Option {
	return func(w *options) {
		w.runFunc = fn
	}
}

// WithLogger set custom logger
func WithLogger(l queue.Logger) Option {
	return func(w *options) {
		w.logger = l
	}
}

func newOptions(opts ...Option) options {
	defaultOpts := options{
		addr:         "amqp://guest:guest@localhost:5672/",
		queue:        "golang-queue",
		tag:          "golang-queue",
		exchangeName: "test-exchange",
		exchangeType: ExchangeDirect,
		routingKey:   "test-key",
		logger:       queue.NewLogger(),
		autoAck:      false,
		runFunc: func(context.Context, core.QueuedMessage) error {
			return nil
		},
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt(&defaultOpts)
	}

	if !isVaildExchange(defaultOpts.exchangeType) {
		defaultOpts.logger.Fatal("invaild exchange type: ", defaultOpts.exchangeType)
	}

	return defaultOpts
}
