package rabbitmq

import (
	"context"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

// Option for queue system
type Option func(*options)

type options struct {
	runFunc func(context.Context, core.QueuedMessage) error
	logger  queue.Logger
	addr    string
	subj    string
	tag     string
}

// WithAddr setup the URI
func WithAddr(addr string) Option {
	return func(w *options) {
		w.addr = "nats://" + addr
	}
}

// WithAddr setup the tag
func WithTag(tag string) Option {
	return func(w *options) {
		w.tag = tag
	}
}

// WithSubj setup the topic
func WithSubj(subj string) Option {
	return func(w *options) {
		w.subj = subj
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
		addr:   "amqp://guest:guest@localhost:5672/",
		subj:   "queue",
		tag:    "golang-queue",
		logger: queue.NewLogger(),
		runFunc: func(context.Context, core.QueuedMessage) error {
			return nil
		},
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt(&defaultOpts)
	}

	return defaultOpts
}
