package main

import (
	"fmt"
	"time"
	"reflect"
	"github.com/valinurovam/tinyqueue"
	"context"
)

func main() {
	tinyQueue := tinyqueue.New()
	tinyQueue.CreateExchange("test_exchange_1")
	tinyQueue.CreateExchange("test_exchange_2")
	tinyQueue.CreateQueue("test_queue_1")
	tinyQueue.CreateQueue("test_queue_2")
	tinyQueue.CreateQueue("test_queue_3")
	tinyQueue.Bind("test_exchange_1", "test_queue_1", "test")
	tinyQueue.Bind("test_exchange_1", "test_queue_2", "test_(queue|2)")

	tinyQueue.Bind("test_exchange_2", "test_queue_3", "test3\\w+")

	go func() {
		for {
			tinyQueue.SendMessage("test_exchange_1", tinyqueue.Message{Body: "message for test_queue_1", RoutingKey: "test"})
			tinyQueue.SendMessage("test_exchange_1", tinyqueue.Message{Body: func() { fmt.Println("bla bla bla") }, RoutingKey: "test"})
			tinyQueue.SendMessage("test_exchange_1", tinyqueue.Message{Body: "message should be skipped", RoutingKey: "test_and_test"})
			tinyQueue.SendMessage("test_exchange_1", tinyqueue.Message{Body: "message for test_queue_2", RoutingKey: "test_queue"})
			tinyQueue.SendMessage("test_exchange_1", tinyqueue.Message{Body: "message for test_queue_2", RoutingKey: "test_2"})
			tinyQueue.SendMessage("test_exchange_2", tinyqueue.Message{Body: "message should be skipped", RoutingKey: "test3"})
			tinyQueue.SendMessage("test_exchange_2", tinyqueue.Message{Body: "message for test_queue_3", RoutingKey: "test3_foo"})
			tinyQueue.SendMessage("test_exchange_2", tinyqueue.Message{Body: "message for test_queue_3", RoutingKey: "test3_bar_foo"})
			//time.Sleep(100 * time.Millisecond)
		}

	}()

	go consumeQueue("test_queue_1", tinyQueue)
	go consumeQueue("test_queue_2", tinyQueue)
	go consumeQueue("test_queue_3", tinyQueue)

	time.Sleep(time.Hour)
}

func consumeQueue(queueName tinyqueue.QueueName, queue *tinyqueue.TinyQueue) {
	var consumer *tinyqueue.Consumer
	var err error

	if consumer, err = queue.Consume(queueName); err != nil {
		fmt.Println("Something going wrong, err: " + err.Error())
		return
	}

	for {
		message, _ := consumer.Read(context.Background())

		switch message.Body.(type) {
		case func():
			reflect.ValueOf(message.Body).Call([]reflect.Value{})
		default:
			fmt.Println(message)
		}

		consumer.Deliver(message.Ack())
	}
}
