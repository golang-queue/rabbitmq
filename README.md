# rabbitmq

[![Run Testing](https://github.com/golang-queue/rabbitmq/actions/workflows/go.yml/badge.svg)](https://github.com/golang-queue/rabbitmq/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/golang-queue/rabbitmq)](https://goreportcard.com/report/github.com/golang-queue/rabbitmq)
[![GoDoc](https://pkg.go.dev/badge/github.com/golang-queue/rabbitmq)](https://pkg.go.dev/github.com/golang-queue/rabbitmq)

[RabbitMQ](https://www.rabbitmq.com/) as backend for Queue Package. See the [Go RabbitMQ Client Library](https://github.com/rabbitmq/amqp091-go) maintained by the [RabbitMQ core team](https://github.com/rabbitmq). It was [originally developed by Sean Treadway](https://github.com/streadway/amqp).

## Exchanges and Exchange Types

See the [Exchanges and Exchange Types][11] section of [AMQP 0-9-1 Model Explained][12].

[11]: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
[12]: https://www.rabbitmq.com/tutorials/amqp-concepts.html

### Direct Exchange

![direct-exchange](./images/exchange-direct.png)

See the consumer code:

```go
package main

import (
  "context"
  "encoding/json"
  "flag"
  "fmt"
  "log"
  "time"

  "github.com/golang-queue/queue"
  "github.com/golang-queue/queue/core"
  rabbitmq "github.com/golang-queue/rabbitmq"
)

type job struct {
  Message string
}

func (j *job) Bytes() []byte {
  b, err := json.Marshal(j)
  if err != nil {
    panic(err)
  }
  return b
}

var (
  uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
  exchange     = flag.String("exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
  exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
  q            = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
  bindingKey   = flag.String("key", "test-key", "AMQP binding key")
)

func init() {
  flag.Parse()
}

func main() {
  taskN := 100
  rets := make(chan string, taskN)

  // define the worker
  w := rabbitmq.NewWorker(
    rabbitmq.WithAddr(*uri),
    rabbitmq.WithQueue(*q),
    rabbitmq.WithExchangeName(*exchange),
    rabbitmq.WithRoutingKey(*bindingKey),
    rabbitmq.WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
      var v *job
      if err := json.Unmarshal(m.Payload(), &v); err != nil {
        return err
      }
      rets <- v.Message
      return nil
    }),
  )

  // define the queue
  q, err := queue.NewQueue(
    queue.WithWorkerCount(5),
    queue.WithWorker(w),
  )
  if err != nil {
    log.Fatal(err)
  }

  // start the five worker
  q.Start()

  // assign tasks in queue
  for i := 0; i < taskN; i++ {
    go func(i int) {
      if err := q.Queue(&job{
        Message: fmt.Sprintf("handle the job: %d", i+1),
      }); err != nil {
        log.Fatal(err)
      }
    }(i)
  }

  // wait until all tasks done
  for i := 0; i < taskN; i++ {
    fmt.Println("message:", <-rets)
    time.Sleep(50 * time.Millisecond)
  }

  // shutdown the service and notify all the worker
  q.Release()
}
```
