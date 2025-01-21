# Producer-Consumer Example with RabbitMQ

This example demonstrates a simple producer-consumer pattern using RabbitMQ in Go.

## Prerequisites

- Go 1.22 or later
- RabbitMQ server

## Installation

1. Clone the repository:

```sh
git clone https://github.com/appleboy/golang-queue.git
cd golang-queue/rabbitmq/_example/producer-consumer
```

2. Install dependencies:

```sh
go mod tidy
```

## Running the Example

### Start RabbitMQ

Ensure RabbitMQ server is running. You can start RabbitMQ using Docker:

```sh
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### Producer

The producer sends messages to the RabbitMQ queue.

1. Navigate to the producer directory:

```sh
cd producer
```

2. Run the producer:

```sh
go run main.go
```

### Consumer

The consumer receives messages from the RabbitMQ queue.

1. Navigate to the consumer directory:

```sh
cd consumer
```

2. Run the consumer:

```sh
go run main.go
```
