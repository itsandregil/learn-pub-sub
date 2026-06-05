package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueType string

const (
	QueueTypeDurable   QueueType = "durable"
	QueueTypeTransient QueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType QueueType) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open channel: %w", err)
	}
	var queue amqp.Queue
	switch queueType {
	case QueueTypeDurable:
		queue, err = channel.QueueDeclare(queueName, true, false, false, false, nil)
	case QueueTypeTransient:
		queue, err = channel.QueueDeclare(queueName, false, true, true, false, nil)
	}
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare queue: %w", err)
	}
	err = channel.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue: %w", err)
	}
	return channel, queue, nil
}
