package rabbitmq

import (
	"context"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

/*
Package rabbitmq provides configuration options and helper functions
for setting up and customizing RabbitMQ workers and queues in the golang-queue system.
This file defines the available exchange types, the options struct, and a set of functional
options for flexible configuration.
*/

/*
Predefined RabbitMQ exchange types for use in configuration.
- ExchangeDirect: Direct exchange type.
- ExchangeFanout: Fanout exchange type.
- ExchangeTopic: Topic exchange type.
- ExchangeHeaders: Headers exchange type.
*/
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

/*
isVaildExchange checks if the provided exchange name is one of the supported types.

Parameters:
- name: The exchange type name to validate.

Returns:
- bool: true if the exchange type is valid, false otherwise.
*/
func isVaildExchange(name string) bool {
	switch name {
	case ExchangeDirect, ExchangeFanout, ExchangeTopic, ExchangeHeaders:
		return true
	default:
		return false
	}
}

/*
Option is a functional option type for configuring the options struct.
It allows for flexible and composable configuration of RabbitMQ workers and queues.
*/
type Option func(*options)

/*
options struct holds all configuration parameters for a RabbitMQ worker or queue.

Fields:
- runFunc: The function to execute for each task.
- logger: Logger instance for logging.
- addr: AMQP server URI.
- queue: Name of the queue to use.
- tag: Consumer tag for identification.
- exchangeName: Name of the AMQP exchange.
- exchangeType: Type of the AMQP exchange (direct, fanout, topic, headers).
- autoAck: Whether to enable automatic message acknowledgment.
- routingKey: AMQP routing key for message delivery.
*/
type options struct {
	runFunc      func(context.Context, core.TaskMessage) error
	logger       queue.Logger
	addr         string
	queue        string
	tag          string
	exchangeName string // Durable AMQP exchange name
	exchangeType string // Exchange Types: Direct, Fanout, Topic and Headers
	autoAck      bool
	routingKey   string // AMQP routing key
}

/*
WithAddr sets the AMQP server URI.

Parameters:
- addr: The AMQP URI to connect to.

Returns:
- Option: Functional option to set the address.
*/
func WithAddr(addr string) Option {
	return func(w *options) {
		w.addr = addr
	}
}

/*
WithExchangeName sets the name of the AMQP exchange.

Parameters:
- val: The exchange name.

Returns:
- Option: Functional option to set the exchange name.

Exchanges are AMQP 0-9-1 entities where messages are sent to.
Exchanges take a message and route it into zero or more queues.
*/
func WithExchangeName(val string) Option {
	return func(w *options) {
		w.exchangeName = val
	}
}

/*
WithExchangeType sets the type of the AMQP exchange.

Parameters:
- val: The exchange type (direct, fanout, topic, headers).

Returns:
- Option: Functional option to set the exchange type.

The routing algorithm used depends on the exchange type and rules called bindings.
AMQP 0-9-1 brokers provide four exchange types:
- Direct exchange (Empty string) and amq.direct
- Fanout exchange amq.fanout
- Topic exchange amq.topic
- Headers exchange amq.match (and amq.headers in RabbitMQ)
*/
func WithExchangeType(val string) Option {
	return func(w *options) {
		w.exchangeType = val
	}
}

/*
WithRoutingKey sets the AMQP routing key.

Parameters:
- val: The routing key.

Returns:
- Option: Functional option to set the routing key.
*/
func WithRoutingKey(val string) Option {
	return func(w *options) {
		w.routingKey = val
	}
}

/*
WithTag sets the consumer tag for the worker.

Parameters:
- val: The consumer tag.

Returns:
- Option: Functional option to set the tag.
*/
func WithTag(val string) Option {
	return func(w *options) {
		w.tag = val
	}
}

/*
WithAutoAck enables or disables automatic message acknowledgment.

Parameters:
- val: true to enable auto-ack, false to disable.

Returns:
- Option: Functional option to set autoAck.
*/
func WithAutoAck(val bool) Option {
	return func(w *options) {
		w.autoAck = val
	}
}

/*
WithQueue sets the name of the queue to use.

Parameters:
- val: The queue name.

Returns:
- Option: Functional option to set the queue name.
*/
func WithQueue(val string) Option {
	return func(w *options) {
		w.queue = val
	}
}

/*
WithRunFunc sets the function to execute for each task.

Parameters:
- fn: The function to run for each task message.

Returns:
- Option: Functional option to set the run function.
*/
func WithRunFunc(fn func(context.Context, core.TaskMessage) error) Option {
	return func(w *options) {
		w.runFunc = fn
	}
}

/*
WithLogger sets a custom logger for the worker or queue.

Parameters:
- l: The logger instance.

Returns:
- Option: Functional option to set the logger.
*/
func WithLogger(l queue.Logger) Option {
	return func(w *options) {
		w.logger = l
	}
}

/*
newOptions creates a new options struct with default values,
then applies any provided functional options to override defaults.

Parameters:
- opts: Variadic list of Option functions to customize the configuration.

Returns:
- options: The fully configured options struct.
*/
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
		runFunc: func(context.Context, core.TaskMessage) error {
			return nil
		},
	}

	// Apply each provided option to override defaults
	for _, opt := range opts {
		opt(&defaultOpts)
	}

	// Validate the exchange type
	if !isVaildExchange(defaultOpts.exchangeType) {
		defaultOpts.logger.Fatal("invaild exchange type: ", defaultOpts.exchangeType)
	}

	return defaultOpts
}
