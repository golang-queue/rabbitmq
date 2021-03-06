package rabbitmq

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"

	amqp "github.com/rabbitmq/amqp091-go"
)

var _ core.Worker = (*Worker)(nil)

// Worker for NSQ
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

// NewWorker for struc
func NewWorker(opts ...Option) *Worker {
	var err error
	w := &Worker{
		opts:  newOptions(opts...),
		stop:  make(chan struct{}),
		tasks: make(chan amqp.Delivery),
	}

	w.conn, err = amqp.Dial(w.opts.addr)
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

func (w *Worker) startConsumer() (err error) {
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
			w.opts.logger.Error(err)
			return
		}

		if err := w.channel.QueueBind(q.Name, w.opts.routingKey, w.opts.exchangeName, false, nil); err != nil {
			w.opts.logger.Error("cannot consume without a binding to exchange: ", err)
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
			w.opts.logger.Error("cannot consume from: ", q.Name, err)
		}
	})

	return err
}

func (w *Worker) handle(job *queue.Job) error {
	// create channel with buffer size 1 to avoid goroutine leak
	done := make(chan error, 1)
	panicChan := make(chan interface{}, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), job.Timeout)
	defer func() {
		cancel()
	}()

	// run the job
	go func() {
		// handle panic issue
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		// run custom process function
		done <- w.opts.runFunc(ctx, job)
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case <-ctx.Done(): // timeout reached
		return ctx.Err()
	case <-w.stop: // shutdown service
		// cancel job
		cancel()

		leftTime := job.Timeout - time.Since(startTime)
		// wait job
		select {
		case <-time.After(leftTime):
			return context.DeadlineExceeded
		case err := <-done: // job finish
			return err
		case p := <-panicChan:
			panic(p)
		}
	case err := <-done: // job finish
		return err
	}
}

// Run start the worker
func (w *Worker) Run(task core.QueuedMessage) error {
	data, _ := task.(*queue.Job)

	if err := w.handle(data); err != nil {
		return err
	}

	return nil
}

// Shutdown worker
func (w *Worker) Shutdown() (err error) {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	w.stopOnce.Do(func() {
		close(w.stop)
		if err = w.channel.Cancel(w.opts.tag, true); err != nil {
			w.opts.logger.Error("consumer cancel failed: ", err)
		}
		if err = w.conn.Close(); err != nil {
			w.opts.logger.Error("AMQP connection close error: ", err)
		}
	})

	return err
}

// Queue send notification to queue
func (w *Worker) Queue(job core.QueuedMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	err := w.channel.Publish(
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

// Request a new task
func (w *Worker) Request() (core.QueuedMessage, error) {
	_ = w.startConsumer()
	clock := 0
loop:
	for {
		select {
		case task, ok := <-w.tasks:
			if !ok {
				return nil, queue.ErrQueueHasBeenClosed
			}
			var data queue.Job
			_ = json.Unmarshal(task.Body, &data)
			if !w.opts.autoAck {
				_ = task.Ack(w.opts.autoAck)
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
