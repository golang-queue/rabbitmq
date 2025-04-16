module example_01

go 1.22

require (
	github.com/golang-queue/queue v0.4.0
	github.com/golang-queue/rabbitmq v0.0.2-0.20210822122542-200fdcf19ebf
)

require (
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
)

replace github.com/golang-queue/rabbitmq => ../../
