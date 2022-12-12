package rabbitmq

import "testing"

func Test_SendWithSimpleMode(t *testing.T) {
	sender, err := NewSender("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer sender.close()

	err = sender.queue("simple_mode", false, false).send("hello world")
	if err != nil {
		t.Error(err)
		return
	}
}

func Test_ReceiveWithSimpleMode(t *testing.T) {
	receiver, err := NewReceive("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Error(err)
		return
	}
	defer receiver.close()

	err = receiver.queue("simple_mode", false, false).receive(true)
	if err != nil {
		t.Error(err)
		return
	}
}
