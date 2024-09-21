package easymqtt

import (
	"errors"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type EasyMqtt struct {
	clientOptions *mqtt.ClientOptions
	prefix        string
	client        mqtt.Client
	handlers      map[string]mqtt.MessageHandler
}

func New(
	prefix string,
	address string,
	clientId string,
	handlers map[string]mqtt.MessageHandler,
	connectionLostHandler mqtt.ConnectionLostHandler,
	reconnectingHandler mqtt.ReconnectHandler,
) *EasyMqtt {
	opts := mqtt.NewClientOptions().
		AddBroker(address).
		SetClientID(clientId).
		SetPingTimeout(30 * time.Second).
		SetKeepAlive(20 * time.Second).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(10 * time.Second).
		SetConnectionLostHandler(connectionLostHandler).
		SetReconnectingHandler(reconnectingHandler)

	return NewWithOptions(prefix, handlers, opts)
}

func NewWithOptions(prefix string, handlers map[string]mqtt.MessageHandler, clientOptions *mqtt.ClientOptions) *EasyMqtt {
	return &EasyMqtt{
		prefix:        prefix,
		clientOptions: clientOptions,
		handlers:      handlers,
	}
}

func (em *EasyMqtt) RegisterHandlers(handlers map[string]mqtt.MessageHandler) error {
	if em.client == nil {
		return errors.New("client not started")
	}

	for topic, handler := range handlers {
		if len(em.prefix) > 0 {
			topic = em.prefix + "/" + topic
		}

		fmt.Println("Subscribing to " + topic)
		token := em.client.Subscribe(topic, 2, handler)
		waited := token.Wait()

		if !waited {
			return errors.New("failed to wait on subscription for topic " + topic)
		}

		err := token.Error()
		if err != nil {
			return err
		}
	}

	return nil
}

func (em *EasyMqtt) Start() error {
	if em.client != nil {
		return ErrAlreadyStarted
	}

	client := mqtt.NewClient(em.clientOptions)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	em.client = client

	return em.RegisterHandlers(em.handlers)
}

func (em *EasyMqtt) Send(topic string, payload interface{}) error {
	token := em.client.Publish(
		topic,
		2,
		false,
		payload,
	)

	token.Wait()
	if token.Error() != nil {
		return token.Error()
	}

	return nil
}
