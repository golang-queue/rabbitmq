package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	"github.com/golang-queue/queue/job"

	amqp "github.com/rabbitmq/amqp091-go"
)

var _ core.Worker = (*Worker)(nil)

// ReconnectConfig defines the retry policy for RabbitMQ connection.
type ReconnectConfig struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// dialWithRetry tries to connect to RabbitMQ with retry and backoff.
func dialWithRetry(addr string, cfg ReconnectConfig) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error
	delay := cfg.InitialDelay
	for i := 0; i < cfg.MaxRetries; i++ {
		conn, err = amqp.Dial(addr)
		if err == nil {
			return conn, nil
		}
		time.Sleep(delay)
		// Exponential backoff with cap
		delay = time.Duration(math.Min(float64(cfg.MaxDelay), float64(delay)*2))
	}
	return nil, errors.New("failed to connect to RabbitMQ after retries: " + err.Error())
}

/*
Worker struct implements the core.Worker interface for RabbitMQ.
It manages the AMQP connection, channel, and task consumption.
Fields:
- conn: AMQP connection to RabbitMQ server.
- channel: AMQP channel for communication.
- stop: Channel to signal worker shutdown.
- stopFlag: Atomic flag to indicate if the worker is stopped.
- stopOnce: Ensures shutdown logic runs only once.
- startOnce: Ensures consumer initialization runs only once.
- opts: Configuration options for the worker.
- tasks: Channel for receiving AMQP deliveries (tasks).
*/
type Worker struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	stop      chan struct{}
	stopFlag  int32
	stopOnce  sync.Once
	startOnce sync.Once
	opts      options
	tasks     <-chan amqp.Delivery
}

/*
NewWorker creates and initializes a new Worker instance with the provided options.
It establishes a connection to RabbitMQ, sets up the channel, and declares the exchange.
If any step fails, it logs a fatal error and terminates the process.

Parameters:
- opts: Variadic list of Option functions to configure the worker.

Returns:
- Pointer to the initialized Worker.
*/
func NewWorker(opts ...Option) *Worker {
	var err error
	w := &Worker{
		opts:  newOptions(opts...),
		stop:  make(chan struct{}),
		tasks: make(chan amqp.Delivery),
	}

	// Use retry config, fallback to default if not set
	retryCfg := ReconnectConfig{
		MaxRetries:   5,
		InitialDelay: 500 * time.Millisecond,
		MaxDelay:     5 * time.Second,
	}
	w.conn, err = dialWithRetry(w.opts.addr, retryCfg)
	if err != nil {
		w.opts.logger.Fatal("can't connect rabbitmq: ", err)
	}

	w.channel, err = w.conn.Channel()
	if err != nil {
		w.opts.logger.Fatal("can't setup channel: ", err)
	}

	if err := w.channel.ExchangeDeclare(
		w.opts.exchangeName, // name
		w.opts.exchangeType, // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		w.opts.logger.Fatal("can't declares an exchange: ", err)
	}

	return w
}

/*
startConsumer initializes the consumer for the worker.
It declares the queue, binds it to the exchange, and starts consuming messages.
This method is safe to call multiple times but will only execute once due to sync.Once.

Returns:
- error: Any error encountered during initialization, or nil on success.
*/
func (w *Worker) startConsumer() error {
	var initErr error
	w.startOnce.Do(func() {
		q, err := w.channel.QueueDeclare(
			w.opts.queue, // name
			true,         // durable
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			nil,          // arguments
		)
		if err != nil {
			initErr = err
			w.opts.logger.Error("QueueDeclare failed: ", err)
			return
		}

		if err := w.channel.QueueBind(q.Name, w.opts.routingKey, w.opts.exchangeName, false, nil); err != nil {
			initErr = err
			w.opts.logger.Error("QueueBind failed: ", err)
			return
		}

		w.tasks, err = w.channel.Consume(
			q.Name,         // queue
			w.opts.tag,     // consumer
			w.opts.autoAck, // auto-ack
			false,          // exclusive
			false,          // no-local
			false,          // no-wait
			nil,            // args
		)
		if err != nil {
			initErr = err
			w.opts.logger.Error("Consume failed: ", err)
			return
		}
	})

	return initErr
}

/*
Run executes the worker's task processing function.
It delegates the actual task handling to the configured runFunc.

Parameters:
- ctx: Context for cancellation and timeout.
- task: The task message to process.

Returns:
- error: Any error returned by the runFunc.
*/
func (w *Worker) Run(ctx context.Context, task core.TaskMessage) error {
	return w.opts.runFunc(ctx, task)
}

/*
Shutdown gracefully stops the worker.
It ensures shutdown logic runs only once, cancels the consumer, and closes the AMQP connection.
If the worker is already stopped, it returns queue.ErrQueueShutdown.

Returns:
- error: Any error encountered during shutdown, or nil on success.
*/
func (w *Worker) Shutdown() (err error) {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	w.stopOnce.Do(func() {
		close(w.stop)
		// Cancel consumer first
		if w.channel != nil {
			if cerr := w.channel.Cancel(w.opts.tag, true); cerr != nil {
				w.opts.logger.Error("consumer cancel failed: ", cerr)
				if err == nil {
					err = cerr
				}
			}
			// Try to close channel
			if cerr := w.channel.Close(); cerr != nil {
				w.opts.logger.Error("AMQP channel close error: ", cerr)
				if err == nil {
					err = cerr
				}
			}
		}
		// Then close connection
		if w.conn != nil {
			if cerr := w.conn.Close(); cerr != nil {
				w.opts.logger.Error("AMQP connection close error: ", cerr)
				if err == nil {
					err = cerr
				}
			}
		}
	})

	return err
}

/*
Queue publishes a new task message to the RabbitMQ exchange.
If the worker is stopped, it returns queue.ErrQueueShutdown.

Parameters:
- job: The task message to be published.

Returns:
- error: Any error encountered during publishing, or nil on success.
*/
func (w *Worker) Queue(job core.TaskMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	err := w.channel.PublishWithContext(
		context.Background(),
		w.opts.exchangeName, // exchange
		w.opts.routingKey,   // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            job.Bytes(),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		})

	return err
}

/*
Request retrieves a new task message from the queue.
It starts the consumer if not already started, waits for a message, and unmarshals it into a job.Message.
If no message is received within a timeout, it returns queue.ErrNoTaskInQueue.

Returns:
- core.TaskMessage: The received task message, or nil if none.
- error: Any error encountered, or queue.ErrNoTaskInQueue if no task is available.
*/
func (w *Worker) Request() (core.TaskMessage, error) {
	if err := w.startConsumer(); err != nil {
		return nil, err
	}
	clock := 0
loop:
	for {
		select {
		case task, ok := <-w.tasks:
			if !ok {
				return nil, queue.ErrQueueHasBeenClosed
			}
			var data job.Message
			_ = json.Unmarshal(task.Body, &data)
			if !w.opts.autoAck {
				if err := task.Ack(false); err != nil {
					w.opts.logger.Error("Ack failed: ", err)
					_ = task.Nack(false, true) // requeue
				}
			}
			return &data, nil
		case <-time.After(1 * time.Second):
			if clock == 5 {
				break loop
			}
			clock += 1
		}
	}

	return nil, queue.ErrNoTaskInQueue
}
