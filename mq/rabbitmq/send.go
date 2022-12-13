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
	Err   error
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

/*
# 声明交换机
@param string name 交换机名称
@param string kind 交换机模式 direct/fanout/topic
@param bool durable 交换机是否持久化
@param bool autoDelete 交换机是否自动删除
@param bool internal 是否为内置交换机，如果是只能通过交换机到交换机
@param bool noWait 是否需要等待服务器返回确认响应
*/
func (s *Sender) exchange(name, kind string, durable, autoDelete, internal, noWait bool) *Sender {
	if s.Err != nil {
		return s
	}

	err := s.Ch.ExchangeDeclare(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		nil,
	)
	if err != nil {
		s.Err = errors.New("Failed to declare an exchange: " + err.Error())
		return s
	}

	return s
}

/*
# 声明队列
@param string name 队列名称
@param bool durable 队列是否持久化（只是队列持久化,不代表队列中的消息持久化）
@param bool autoDelete 队列是否自动删除
@param bool exclusive 队列是否专属
@param bool noWait 是否需要等待服务器返回确认响应
*/
func (s *Sender) queue(name string, durable, autoDelete, exclusive, noWait bool) *Sender {
	if s.Err != nil {
		return s
	}
	q, err := s.Ch.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		nil,
	)

	if err != nil {
		s.Err = errors.New("failed to declare a queue: " + err.Error())
	} else {
		s.Queue = q
	}

	return s
}

/*
# 发送消息
@param string exchangeName 交换机名称
@param string routingKey 路由key
@param string body 消息
@param bool mandatory 投递失败是否返回给生产者
@param bool immediate 3.0后不支持
*/
func (s *Sender) send(exchangeName, routingKey string, mandatory bool, body string) *Sender {
	if s.Err != nil {
		return s
	}
	// 定义上下文
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Ch.PublishWithContext(ctx,
		exchangeName,
		routingKey,
		mandatory,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		s.Err = errors.New("failed to publish a message: " + err.Error())
		return s
	}
	log.Printf(" [x] Sent %s\n", body)

	return s
}

// 错误
func (s *Sender) err() error {
	return s.Err
}

// 关闭连接
func (s *Sender) close() {
	_ = s.Ch.Close()
	_ = s.Conn.Close()
}
