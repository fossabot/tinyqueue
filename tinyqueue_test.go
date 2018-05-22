package tinyqueue_test

import (
	"reflect"
	"testing"
	"github.com/valinurovam/tinyqueue"
	"time"
	"context"
)

func TestTinyQueue_New(t *testing.T) {
	queue := tinyqueue.New()
	actial := reflect.TypeOf(queue)
	expected := reflect.TypeOf(&tinyqueue.TinyQueue{})

	if actial != expected {
		t.Error(
			"For", "Create new TiniQueue instance",
			"expected", expected,
			"got", actial,
		)
	}
}

func TestTinyQueue_CreateExchange_Positive(t *testing.T) {
	queue := tinyqueue.New()
	if err := queue.CreateExchange("any"); err != nil {
		t.Error(
			"For", "Create exchange",
			"expected", nil,
			"got", err,
		)
	}
}

func TestTinyQueue_CreateExchange_ExistingReturnError(t *testing.T) {
	queue := tinyqueue.New()
	queue.CreateExchange("any")
	if err := queue.CreateExchange("any"); err == nil {
		t.Error(
			"For", "Create exising exchange",
			"expected", "error",
			"got", err,
		)
	}
}

func TestTinyQueue_CreateQueue_Positive(t *testing.T) {
	queue := tinyqueue.New()
	if err := queue.CreateQueue("any"); err != nil {
		t.Error(
			"For", "Create queue",
			"expected", nil,
			"got", err,
		)
	}
}

func TestTinyQueue_CreateQueue_ExistingReturnError(t *testing.T) {
	queue := tinyqueue.New()
	queue.CreateQueue("any")
	if err := queue.CreateQueue("any"); err == nil {
		t.Error(
			"For", "Create exising queue",
			"expected", "error",
			"got", err,
		)
	}
}

func TestTinyQueue_Bind_Positive(t *testing.T) {
	queue := tinyqueue.New()
	queue.CreateExchange("any")
	queue.CreateQueue("any")
	if err := queue.Bind("any", "any", "any"); err != nil {
		t.Error(
			"For", "Create bind",
			"expected", nil,
			"got", err,
		)
	}
}

func TestTinyQueue_Bind_ExistingReturnError(t *testing.T) {
	queue := tinyqueue.New()
	queue.CreateExchange("any")
	queue.CreateQueue("any")
	queue.Bind("any", "any", "any")

	if err := queue.Bind("any", "any", "any"); err == nil {
		t.Error(
			"For", "Create existing bind",
			"expected", "error",
			"got", err,
		)
	}
}

func TestTinyQueue_SendMessage_Positive(t *testing.T) {
	queue := tinyqueue.New()
	queue.CreateExchange("any")
	queue.CreateQueue("any")
	queue.Bind("any", "any", "any")

	// with existing bind by routing key
	if err := queue.SendMessage("any", tinyqueue.Message{Body: "message for any", RoutingKey: "any"}); err != nil {
		t.Error(
			"For", "Send message with existing bind by routing key",
			"expected", nil,
			"got", err,
		)
	}

	// with non existing bind by routing key
	if err := queue.SendMessage("any", tinyqueue.Message{Body: "message for any", RoutingKey: "test"}); err != nil {
		t.Error(
			"For", "Send message with non existing bind by routing key",
			"expected", nil,
			"got", err,
		)
	}
}

func TestTinyQueue_SendMessage_NonExistingExchangeError(t *testing.T) {
	queue := tinyqueue.New()
	queue.CreateExchange("any")
	queue.CreateQueue("any")
	queue.Bind("any", "any", "any")

	if err := queue.SendMessage("test", tinyqueue.Message{Body: "message for any", RoutingKey: "any"}); err == nil {
		t.Error(
			"For", "Send message to non existing exchange",
			"expected", "error",
			"got", err,
		)
	}
}

func TestTinyQueue_Consume(t *testing.T) {
	queue := tinyqueue.New()
	queue.CreateExchange("any")
	queue.CreateQueue("any")
	queue.Bind("any", "any", "any")

	messages := []tinyqueue.Message{
		{Body: 1, RoutingKey: "any"},
		{Body: 1.2, RoutingKey: "any"},
		{Body: func() {}, RoutingKey: "any"},
		{Body: time.Now(), RoutingKey: "any"},
		{Body: "message for any", RoutingKey: "any"},
	}

	for _, message := range messages {
		queue.SendMessage("any", message)
	}

	var consumer *tinyqueue.Consumer
	var err error

	if consumer, err = queue.Consume("any"); err != nil {
		t.Error(err)
	}

	// looks little strange, but we need to leave from loop
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var consumedMessages []tinyqueue.Message

	for {
		var message *tinyqueue.Message
		if message, err = consumer.Read(ctx); err != nil {
			break
		}

		consumedMessages = append(consumedMessages, *message)
		consumer.Deliver(message.Ack())
	}

	if len(consumedMessages) != len(messages) {
		t.Error(
			"For", "Consumed messages not equal",
			"expected", messages,
			"got", consumedMessages,
		)
	}

	for i := 0; i < len(consumedMessages); i++ {
		if reflect.ValueOf(messages[i].Body) != reflect.ValueOf(consumedMessages[i].Body) {
			t.Error(
				"For", "Consumed messages not equal",
				"expected", messages,
				"got", consumedMessages,
			)
		}
	}
}
