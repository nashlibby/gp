package rabbitmq

import (
	"log"
	"testing"
)

// # 经典模式生产端测试
// 1. 直接按队列名称投递
// 2. 交换机名称不用填
// 3. 路由key填队列名称
func Test_SendWithClassicMode(t *testing.T) {
	sender, err := NewSender("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer sender.close()

	exchangeName := ""
	queueName := "queue_classic"

	err = sender.queue(queueName, true, false, false, false).
		send(exchangeName, queueName, false, "hello world").err()

	if err != nil {
		t.Error(err)
		return
	}
}

// # 经典模式消费端测试
// 1. 直接按队列名称消费
// 2. 不用申明交互机和绑定
func Test_ReceiveWithClassicMode(t *testing.T) {
	receiver, err := NewReceive("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer receiver.close()

	queueName := "queue_classic"

	err = receiver.queue(queueName, true, false, false, false).
		receive(queueName,
			"",
			true,
			false,
			false,
			false,
			func(msg []byte) {
				log.Printf("Received a message: %s", msg)
			}).err()

	if err != nil {
		t.Error(err)
		return
	}
}

// # fanout模式生产端测试（子网广播，发布订阅）
// 1. 指定交换机名称
// 2. 直接交换机模式为fanout
// 3. 不需要指定routingKey
// 4. 不需要申明队列
func Test_SendWithFanoutMode(t *testing.T) {
	sender, err := NewSender("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer sender.close()

	exchangeName := "logs"
	exchangeType := "fanout"
	routingKey := ""

	err = sender.
		exchange(exchangeName, exchangeType, true, false, false, false).
		send(exchangeName, routingKey, false, "hello world").err()

	if err != nil {
		t.Error(err)
		return
	}
}

// # fanout模式消费端测试（子网广播，发布订阅）
// # 只要绑定到该交换机的队列都能消费到
// 1. 指定交换机名称
// 2. 直接交换机模式为fanout
// 3. 不需要指定routingKey
// 4. 队列名称自定义
// 5. 需要绑定routingKey
func Test_ReceiveWithFanoutMode(t *testing.T) {
	receiver, err := NewReceive("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer receiver.close()

	exchangeName := "logs"
	exchangeType := "fanout"
	queueName := "random"
	routingKey := ""

	err = receiver.
		exchange(exchangeName, exchangeType, true, false, false, false).
		queue(queueName, true, false, false, false).
		bind(queueName, routingKey, exchangeName, false).
		receive(queueName, "", true, false, false, false, func(msg []byte) {
			log.Printf("Received a message: %s", msg)
		}).err()

	if err != nil {
		t.Error(err)
		return
	}
}

// # direct模式生产端测试（点对点，路由key完全匹配）
// 1. 指定交换机名称
// 2. 直接交换机模式为direct
// 3. 指定routingKey
// 4. 不需要申明队列
func Test_SendWithDirectMode(t *testing.T) {
	sender, err := NewSender("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer sender.close()

	exchangeName := "logs_direct"
	exchangeType := "direct"
	routingKey := "log.info"

	err = sender.
		exchange(exchangeName, exchangeType, true, false, false, false).
		send(exchangeName, routingKey, false, "hello world").err()

	if err != nil {
		t.Error(err)
		return
	}
}

// # direct模式消费端测试（点对点，路由key完全匹配）
// 1. 指定交换机名称
// 2. 直接交换机模式为direct
// 3. 指定routingKey
// 4. 队列名称可自定义
// 5. 需要绑定routingKey
func Test_ReceiveWithDirectMode(t *testing.T) {
	receiver, err := NewReceive("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer receiver.close()

	exchangeName := "logs_direct"
	exchangeType := "direct"
	routingKey := "log.info"
	queueName := "info"

	err = receiver.
		exchange(exchangeName, exchangeType, true, false, false, false).
		queue(queueName, true, false, false, false).
		bind(queueName, routingKey, exchangeName, false).
		receive(queueName, "", true, false, false, false, func(msg []byte) {
			log.Printf("Received a message: %s", msg)
		}).err()

	if err != nil {
		t.Error(err)
		return
	}
}

// # topic模式生产端测试（路由key规则匹配）
// 1. 指定交换机名称
// 2. 直接交换机模式为topic
// 3. 指定routingKey
// 4. 不需要申明队列
func Test_SendWithTopicMode(t *testing.T) {
	sender, err := NewSender("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer sender.close()

	exchangeName := "logs_topic"
	exchangeType := "topic"
	routingKey := "log.info"

	err = sender.
		exchange(exchangeName, exchangeType, true, false, false, false).
		send(exchangeName, routingKey, false, "hello world").err()

	if err != nil {
		t.Error(err)
		return
	}
}

// # topic模式消费端测试（路由key规则匹配）
// 1. 指定交换机名称
// 2. 直接交换机模式为topic
// 3. 指定routingKey (#表示匹配一个或者多个 *表示匹配一个)
// 4. 队列名称可自定义
// 5. 需要绑定routingKey
func Test_ReceiveWithTopicMode(t *testing.T) {
	receiver, err := NewReceive("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer receiver.close()

	exchangeName := "logs_topic"
	exchangeType := "topic"
	routingKey := "log.*"
	queueName := "info"

	err = receiver.
		exchange(exchangeName, exchangeType, true, false, false, false).
		queue(queueName, true, false, false, false).
		bind(queueName, routingKey, exchangeName, false).
		receive(queueName, "", true, false, false, false, func(msg []byte) {
			log.Printf("Received a message: %s", msg)
		}).err()

	if err != nil {
		t.Error(err)
		return
	}
}
