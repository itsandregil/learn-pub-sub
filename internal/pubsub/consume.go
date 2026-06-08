package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueType string

const (
	QueueTypeDurable   QueueType = "durable"
	QueueTypeTransient QueueType = "transient"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType QueueType) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open channel: %w", err)
	}
	queue, err := channel.QueueDeclare(
		queueName,
		queueType == QueueTypeDurable,
		queueType != QueueTypeDurable,
		queueType != QueueTypeDurable,
		false,
		amqp.Table{"x-dead-letter-exchange": "peril_dlx"},
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare queue: %w", err)
	}
	err = channel.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue: %w", err)
	}
	return channel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType QueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(body []byte) (T, error) {
		var data T
		err := json.Unmarshal(body, &data)
		return data, err
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType QueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(body []byte) (T, error) {
		var data T
		decoder := gob.NewDecoder(bytes.NewBuffer(body))
		err := decoder.Decode(&data)
		return data, err
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType QueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failed to declare and bind queue: %w", err)
	}
	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}
	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume: %w", err)
	}
	go func() {
		defer ch.Close()
		for msg := range msgs {
			data, err := unmarshaller(msg.Body)
			if err != nil {
				log.Printf("failed to unmarshal message: %v", err)
				continue
			}
			switch ack := handler(data); ack {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			}
		}
	}()
	return nil
}
