package tinyqueue

import (
	"sync"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	"regexp"
)

const (
	ACK     = iota + 1
	REQUEUE
)

type ExchangeName = string
type QueueName = string
type RoutingKey = string

type Message struct {
	id         uint64
	Body       interface{}
	RoutingKey RoutingKey
}

type DeliveryMessage struct {
	id     uint64
	status uint8
}

func (m Message) Ack() DeliveryMessage {
	return DeliveryMessage{
		id:     m.id,
		status: ACK,
	}
}

func (m Message) Requeue() DeliveryMessage {
	return DeliveryMessage{
		id:     m.id,
		status: REQUEUE,
	}
}

type Exchange struct {
	exchange chan Message
	bindings map[*regexp.Regexp]QueueName
}

type Queue struct {
	quMu          sync.RWMutex
	queue         []Message
	deliveryQueue map[uint64]Message

	name     QueueName
	sequence uint64
}

type TinyQueue struct {
	eMu       sync.RWMutex
	exchanges map[ExchangeName]*Exchange
	qMu       sync.RWMutex
	queues    map[QueueName]*Queue
}

func New() (instance *TinyQueue) {
	return &TinyQueue{
		exchanges: make(map[ExchangeName]*Exchange),
		queues:    make(map[QueueName]*Queue),
	}
}

func (d *TinyQueue) getExchange(exchangeName ExchangeName) (exchange *Exchange, err error) {
	var ok bool

	d.eMu.RLock()
	defer d.eMu.RUnlock()
	if exchange, ok = d.exchanges[exchangeName]; !ok {
		err = errors.New(fmt.Sprintf("Exchange '%s' does not exist", exchangeName))
	}

	return
}

func (d *TinyQueue) getQueue(queueName QueueName) (queue *Queue, err error) {
	var ok bool

	d.qMu.RLock()
	defer d.qMu.RUnlock()
	if queue, ok = d.queues[queueName]; !ok {
		err = errors.New(fmt.Sprintf("Queue '%s' does not exist", queueName))
	}
	return
}

func (d *TinyQueue) CreateExchange(name ExchangeName) error {
	d.eMu.Lock()
	defer d.eMu.Unlock()
	if _, ok := d.exchanges[name]; ok {
		return errors.New(fmt.Sprintf("Exchange '%s' already exists", name))
	}

	exchange := &Exchange{
		exchange: make(chan Message),
		bindings: make(map[*regexp.Regexp]QueueName),
	}
	d.exchanges[name] = exchange
	go d.routeMessages(exchange)
	return nil
}

func (d *TinyQueue) CreateQueue(name QueueName) error {
	d.qMu.Lock()
	defer d.qMu.Unlock()
	if _, ok := d.queues[name]; ok {
		return errors.New(fmt.Sprintf("Queue '%s' already exists", name))
	}

	queue := &Queue{
		name:          name,
		deliveryQueue: make(map[uint64]Message),
	}
	d.queues[name] = queue
	return nil
}

func (d *TinyQueue) Bind(exchangeName ExchangeName, queueName QueueName, routingKey RoutingKey) (err error) {
	exchange, err := d.getExchange(exchangeName)
	if err != nil {
		return
	}

	_, err = d.getQueue(queueName)
	if err != nil {
		return
	}

	expression := fmt.Sprintf("^%s$", routingKey)
	rxp, err := regexp.Compile(expression)
	exchange.bindings[rxp] = queueName
	return nil
}

func (d *TinyQueue) SendMessage(exchangeName ExchangeName, message Message) (err error) {
	exchange, err := d.getExchange(exchangeName)
	if err != nil {
		return
	}

	exchange.sendMessage(message)
	return
}

func (e *Exchange) sendMessage(message Message) {
	e.exchange <- message
}

func (queue *Queue) pushMessage(message Message) {
	message.id = atomic.AddUint64(&queue.sequence, 1)
	queue.queue = append(queue.queue, message)
}

func (queue *Queue) popMessage() Message {
	var message Message
	message, queue.queue = queue.queue[0], queue.queue[1:]
	return message
}

func (d *TinyQueue) routeMessages(e *Exchange) {
	for {
		message := <-e.exchange

		var queueNames []QueueName

		for rxp, queueName := range e.bindings {
			if rxp.MatchString(message.RoutingKey) {
				queueNames = append(queueNames, queueName)
			}
		}

		for _, queueName := range queueNames {
			queue, err := d.getQueue(queueName)
			if err == nil {
				queue.quMu.Lock()
				queue.pushMessage(message)
				queue.quMu.Unlock()
			}
		}
	}
}

func (d *TinyQueue) Consume(queueName QueueName, messages chan Message, delivery chan DeliveryMessage) (err error) {
	queue, err := d.getQueue(queueName)
	if err != nil {
		return
	}

	go func(channel chan Message) {
		var message Message
		for {
			queue.quMu.Lock()
			if len(queue.queue) > 0 {
				message = queue.popMessage()
				queue.deliveryQueue[message.id] = message
				queue.quMu.Unlock()
				channel <- message
			} else {
				queue.quMu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}(messages)

	go func(channel chan DeliveryMessage) {
		var deliveryMessage DeliveryMessage
		for {
			deliveryMessage = <-channel
			queue.quMu.Lock()
			if message, ok := queue.deliveryQueue[deliveryMessage.id]; ok {
				switch deliveryMessage.status {
				case REQUEUE:
					queue.queue = append([]Message{message}, queue.queue...)
				}
				delete(queue.deliveryQueue, deliveryMessage.id)
			}
			queue.quMu.Unlock()
		}
	}(delivery)

	return nil
}
