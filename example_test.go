package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

// Direct Exchange
func Example_direct_exchange() {
	m := mockMessage{
		Message: "foo",
	}
	w1 := NewWorker(
		WithSubj("direct_queue"),
		WithExchangeName("direct_exchange"),
		WithExchangeType("direct"),
		WithRoutingKey("direct_exchange"),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			fmt.Println("worker01 get data:", string(m.Bytes()))
			time.Sleep(100 * time.Millisecond)
			return nil
		}),
	)

	q1, err := queue.NewQueue(
		queue.WithWorker(w1),
	)
	if err != nil {
		w1.opts.logger.Error(err)
	}
	q1.Start()

	w2 := NewWorker(
		WithSubj("direct_queue"),
		WithExchangeName("direct_exchange"),
		WithExchangeType("direct"),
		WithRoutingKey("direct_exchange"),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			fmt.Println("worker02 get data:", string(m.Bytes()))
			time.Sleep(100 * time.Millisecond)
			return nil
		}),
	)

	q2, err := queue.NewQueue(
		queue.WithWorker(w2),
	)
	if err != nil {
		w2.opts.logger.Error(err)
	}
	q2.Start()

	w := NewWorker(
		WithExchangeName("direct_exchange"),
		WithExchangeType("direct"),
		WithRoutingKey("direct_exchange"),
	)

	q, err := queue.NewQueue(
		queue.WithWorker(w),
	)
	if err != nil {
		w.opts.logger.Error(err)
	}

	time.Sleep(200 * time.Millisecond)
	q.Queue(m)
	q.Queue(m)
	q.Queue(m)
	q.Queue(m)
	time.Sleep(200 * time.Millisecond)
	q.Release()
	q1.Release()
	q2.Release()

	// Unordered Output:
	// worker01 get data: foo
	// worker02 get data: foo
	// worker01 get data: foo
	// worker02 get data: foo
}

// Fanout Exchange
func Example_fanout_exchange() {
	m := mockMessage{
		Message: "foo",
	}
	w1 := NewWorker(
		WithSubj("fanout_queue_1"),
		WithExchangeName("fanout_exchange"),
		WithExchangeType("fanout"),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			fmt.Println("worker01 get data:", string(m.Bytes()))
			return nil
		}),
	)

	q1, err := queue.NewQueue(
		queue.WithWorker(w1),
	)
	if err != nil {
		w1.opts.logger.Error(err)
	}
	q1.Start()

	w2 := NewWorker(
		WithSubj("fanout_queue_2"),
		WithExchangeName("fanout_exchange"),
		WithExchangeType("fanout"),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			fmt.Println("worker02 get data:", string(m.Bytes()))
			return nil
		}),
	)

	q2, err := queue.NewQueue(
		queue.WithWorker(w2),
	)
	if err != nil {
		w2.opts.logger.Error(err)
	}
	q2.Start()

	w := NewWorker(
		WithExchangeName("fanout_exchange"),
		WithExchangeType("fanout"),
	)

	q, err := queue.NewQueue(
		queue.WithWorker(w),
	)
	if err != nil {
		w.opts.logger.Error(err)
	}

	time.Sleep(200 * time.Millisecond)
	q.Queue(m)
	time.Sleep(200 * time.Millisecond)
	q.Release()
	q1.Release()
	q2.Release()

	// Unordered Output:
	// worker01 get data: foo
	// worker02 get data: foo
}
