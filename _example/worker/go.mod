module example_01

go 1.18

require (
	github.com/golang-queue/queue v0.1.3
	github.com/golang-queue/rabbitmq v0.0.2-0.20210822122542-200fdcf19ebf
)

require (
	github.com/goccy/go-json v0.9.7 // indirect
	github.com/rabbitmq/amqp091-go v1.3.4 // indirect
)

replace github.com/golang-queue/rabbitmq => ../../
