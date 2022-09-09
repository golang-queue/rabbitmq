GO ?= go

test:
	$(GO) test -v -cover -covermode=atomic -coverprofile=coverage.out ./...

upgrade:
	$(GO) get -u ./...

.PHONY: reset
reset:
	rabbitmqctl stop_app
	rabbitmqctl reset
	rabbitmqctl start_app
