# rabbitmq

[RabbitMQ](https://www.rabbitmq.com/) as backend for Queue Package. See the [Go RabbitMQ Client Library](https://github.com/rabbitmq/amqp091-go) maintained by the [RabbitMQ core team](https://github.com/rabbitmq). It was [originally developed by Sean Treadway](https://github.com/streadway/amqp).

## Exchanges and Exchange Types

See the [Exchanges and Exchange Types][11] section of [AMQP 0-9-1 Model Explained][12].

[11]:https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
[12]:https://www.rabbitmq.com/tutorials/amqp-concepts.html

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
  "time"

  "github.com/appleboy/graceful"
  "github.com/golang-queue/queue"
  "github.com/golang-queue/queue/core"
  "github.com/golang-queue/rabbitmq"
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
  taskN := 10000
  rets := make(chan string, taskN)

  m := graceful.NewManager()

  // define the worker
  w := rabbitmq.NewWorker(
    rabbitmq.WithAddr(*uri),
    rabbitmq.WithQueue(*q),
    rabbitmq.WithExchangeName(*exchange),
    rabbitmq.WithExchangeType(*exchangeType),
    rabbitmq.WithRoutingKey(*bindingKey),
    rabbitmq.WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
      var v *job
      if err := json.Unmarshal(m.Bytes(), &v); err != nil {
        return err
      }
      rets <- v.Message
      time.Sleep(500 * time.Millisecond)
      return nil
    }),
  )
  // define the queue
  q := queue.NewPool(
    2,
    queue.WithWorker(w),
  )

  m.AddRunningJob(func(ctx context.Context) error {
    for {
      select {
      case <-ctx.Done():
        select {
        case m := <-rets:
          fmt.Println("message:", m)
        default:
        }
        return nil
      case m := <-rets:
        fmt.Println("message:", m)
        time.Sleep(50 * time.Millisecond)
      }
    }
  })

  m.AddShutdownJob(func() error {
    // shutdown the service and notify all the worker
    q.Release()
    return nil
  })

  <-m.Done()
}
```
