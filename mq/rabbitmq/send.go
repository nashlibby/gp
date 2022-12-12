package rabbitmq

import (
	"context"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// 生产端
type Sender struct {
	Conn  *amqp.Connection
	Ch    *amqp.Channel
	Queue amqp.Queue
}

func NewSender(url string) (*Sender, error) {
	// 发起连接
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.New("failed to connect to RabbitMQ: " + err.Error())
	}

	// 定义channel
	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.New("failed to open a channel: " + err.Error())
	}
	return &Sender{
		Conn: conn,
		Ch:   ch,
	}, nil
}

// 队列申明
func (s *Sender) queue(name string, durable, autoDelete bool) *Sender {
	s.Queue, _ = s.Ch.QueueDeclare(
		name,       // name
		durable,    // durable
		autoDelete, // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)

	return s
}

// 发送消息
func (s *Sender) send(body string) error {
	// 定义上下文
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Ch.PublishWithContext(ctx,
		"",           // exchange
		s.Queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		return errors.New("failed to publish a message: " + err.Error())
	}
	log.Printf(" [x] Sent %s\n", body)

	return nil
}

// 关闭连接
func (s *Sender) close() {
	_ = s.Ch.Close()
	_ = s.Conn.Close()
}
