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
	Err   error
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

/*
# 声明交换机
@param string name 交换机名称
@param string kind 交换机模式 direct/fanout/topic
@param bool durable 交换机是否持久化
@param bool autoDelete 交换机是否自动删除
@param bool internal 是否为内置交换机，如果是只能通过交换机到交换机
@param bool noWait 是否需要等待服务器返回确认响应
*/
func (r *Receive) exchange(name, kind string, durable, autoDelete, internal, noWait bool) *Receive {
	if r.Err != nil {
		return r
	}

	err := r.Ch.ExchangeDeclare(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		nil,
	)
	if err != nil {
		r.Err = errors.New("Failed to declare an exchange: " + err.Error())
		return r
	}

	return r
}

/*
# 声明队列
@param string name 队列名称
@param bool durable 队列是否持久化（只是队列持久化,不代表队列中的消息持久化）
@param bool autoDelete 队列是否自动删除
@param bool exclusive 队列是否专属
@param bool noWait 是否需要等待服务器返回确认响应
*/
func (r *Receive) queue(name string, durable, autoDelete, exclusive, noWait bool) *Receive {
	if r.Err != nil {
		return r
	}
	q, err := r.Ch.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		nil,
	)

	if err != nil {
		r.Err = errors.New("failed to declare a queue: " + err.Error())
	} else {
		r.Queue = q
	}

	return r
}

/*
# 队列绑定
@param string queueName 队列名称
@param string routingKey 路由key
@param string exchangeName 交换机名称
*/
func (r *Receive) bind(queueName, routingKey, exchangeName string, noWait bool) *Receive {
	if r.Err != nil {
		return r
	}
	err := r.Ch.QueueBind(queueName, routingKey, exchangeName, noWait, nil)
	if err != nil {
		r.Err = errors.New("failed to bind a queue: " + err.Error())
	}

	return r
}

/*
# 消费消息
@param string queueName 队列名称
@param string routingKey 路由key
@param string exchangeName 交换机名称
@param bool autoAck 是否自动应答
@param bool exclusive 是否专属
@param bool noLocal
@param bool noWait 是否需要等待服务器返回确认响应
*/
func (r *Receive) receive(queueName, consumer string, autoAck, exclusive, noLocal, noWait bool, handle func(msg []byte)) *Receive {
	if r.Err != nil {
		return r
	}
	// 注册一个消费端
	messages, err := r.Ch.Consume(
		queueName,
		consumer,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		nil,
	)
	if err != nil {
		r.Err = errors.New("failed to register a consumer: " + err.Error())
		return r
	}

	var forever chan struct{}

	go func() {
		for d := range messages {
			handle(d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	return r
}

// 错误
func (r *Receive) err() error {
	return r.Err
}

// 关闭连接
func (r *Receive) close() {
	_ = r.Ch.Close()
	_ = r.Conn.Close()
}
