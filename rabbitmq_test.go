package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	"github.com/golang-queue/queue/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"
)

const testMessage = "foo"

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type mockMessage struct {
	Message string
}

func (m mockMessage) Bytes() []byte {
	return []byte(m.Message)
}

func setupRabbitMQContainer(ctx context.Context, t *testing.T) (testcontainers.Container, string) {
	req := testcontainers.ContainerRequest{
		Image: "rabbitmq:3-management",
		ExposedPorts: []string{
			"4369/tcp", // epmd
			"5672/tcp", // amqp
		},
		WaitingFor: wait.ForListeningPort("5672/tcp"),
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "guest",
			"RABBITMQ_DEFAULT_PASS": "guest",
		},
	}
	rabbitMQC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	endpoint, err := rabbitMQC.Endpoint(ctx, "")
	require.NoError(t, err)

	return rabbitMQC, endpoint
}

func TestShutdownWorkFlow(t *testing.T) {
	ctx := context.Background()
	rabbitMQC, _ := setupRabbitMQContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, rabbitMQC)

	port, err := rabbitMQC.MappedPort(ctx, "5672")
	require.NoError(t, err)

	host, err := rabbitMQC.Host(ctx)
	require.NoError(t, err)

	w := NewWorker(
		WithQueue("test"),
		WithAddr(fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	require.NoError(t, err)
	q.Start()
	time.Sleep(1 * time.Second)
	q.Shutdown()
	// check shutdown once
	q.Shutdown()
	q.Wait()
}

func TestCustomFuncAndWait(t *testing.T) {
	m := &mockMessage{
		Message: testMessage,
	}
	w := NewWorker(
		WithQueue("test"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			log.Println("show message: " + string(m.Payload()))
			time.Sleep(500 * time.Millisecond)
			return nil
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	require.NoError(t, err)
	q.Start()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, q.Queue(m))
	require.NoError(t, q.Queue(m))
	require.NoError(t, q.Queue(m))
	require.NoError(t, q.Queue(m))
	time.Sleep(600 * time.Millisecond)
	q.Shutdown()
	q.Wait()
	// you will see the execute time > 1000ms
}

func TestEnqueueJobAfterShutdown(t *testing.T) {
	m := mockMessage{
		Message: testMessage,
	}
	w := NewWorker()
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	require.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	q.Shutdown()
	// can't queue task after shutdown
	err = q.Queue(m)
	require.Error(t, err)
	assert.Equal(t, queue.ErrQueueShutdown, err)
	q.Wait()
}

func TestJobReachTimeout(t *testing.T) {
	m := mockMessage{
		Message: testMessage,
	}
	w := NewWorker(
		WithQueue("JobReachTimeout"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Payload()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	require.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, q.Queue(m, job.AllowOption{
		Timeout: job.Time(20 * time.Millisecond),
	}))
	time.Sleep(100 * time.Millisecond)
	q.Shutdown()
	q.Wait()
}

func TestCancelJobAfterShutdown(t *testing.T) {
	m := mockMessage{
		Message: "test",
	}
	w := NewWorker(
		WithQueue("CancelJob"),
		WithLogger(queue.NewLogger()),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Payload()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	require.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, q.Queue(m, job.AllowOption{
		Timeout: job.Time(150 * time.Millisecond),
	}))
	time.Sleep(100 * time.Millisecond)
	q.Shutdown()
	q.Wait()
}

func TestGoroutineLeak(t *testing.T) {
	m := mockMessage{
		Message: testMessage,
	}
	w := NewWorker(
		WithQueue("GoroutineLeak"),
		WithLogger(queue.NewEmptyLogger()),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Payload()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
					log.Println("get data:", string(m.Payload()))
					time.Sleep(50 * time.Millisecond)
					return nil
				}
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithLogger(queue.NewEmptyLogger()),
		queue.WithWorker(w),
		queue.WithWorkerCount(10),
	)
	require.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	for i := range 500 {
		m.Message = fmt.Sprintf("foobar: %d", i+1)
		require.NoError(t, q.Queue(m))
	}
	time.Sleep(200 * time.Millisecond)
	q.Shutdown()
	q.Wait()
	//nolint:forbidigo // intentional diagnostic output for this goroutine-leak check
	fmt.Println("number of goroutines:", runtime.NumGoroutine())
}

func TestGoroutinePanic(t *testing.T) {
	m := mockMessage{
		Message: testMessage,
	}
	w := NewWorker(
		WithQueue("GoroutinePanic"),
		WithRoutingKey("GoroutinePanic"),
		WithExchangeName("GoroutinePanic"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			panic("missing something")
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	require.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, q.Queue(m))
	require.NoError(t, q.Queue(m))
	time.Sleep(2 * time.Second)
	q.Shutdown()
	require.Error(t, q.Queue(m))
	q.Wait()
}
