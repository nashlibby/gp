package rabbitmq

import (
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// 消费端
type Receive struct {
	Conn  *amqp.Connection
	Ch    *amqp.Channel
	Queue amqp.Queue
}

func NewReceive(url string) (*Receive, error) {
	// 发起连接
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.New("failed to connect to RabbitMQ")
	}

	// 定义channel
	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.New("failed to open a channel")
	}
	return &Receive{
		Conn: conn,
		Ch:   ch,
	}, nil
}

// 队列申明
func (r *Receive) queue(name string, durable, autoDelete bool) *Receive {
	r.Queue, _ = r.Ch.QueueDeclare(
		name,       // name
		durable,    // durable
		autoDelete, // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	return r
}

// 消费消息
func (r *Receive) receive(autoAck bool) error {
	// 注册一个消费端
	messages, err := r.Ch.Consume(
		r.Queue.Name, // queue
		"",           // consumer
		autoAck,      // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return errors.New("failed to register a consumer")
	}

	var forever chan struct{}

	go func() {
		for d := range messages {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	return nil
}

// 关闭连接
func (r *Receive) close() {
	_ = r.Ch.Close()
	_ = r.Conn.Close()
}
