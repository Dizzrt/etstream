package kafka

import (
	"errors"
	"sync"

	"github.com/Dizzrt/etfoundation"
	"github.com/Shopify/sarama"
)

var (
	asyncProducerDefaultConfig     sarama.Config
	asyncProducerDefaultConfigInit sync.Once

	ErrInputChannelBlocked = errors.New("input channel blocked")
)

type KafkaWriter struct {
	topic    string
	producer sarama.AsyncProducer
}

type KafkaConfig struct {
	SaramaConfig sarama.Config
	Host         string
	Topic        string
}

func (ws *KafkaWriter) Write(b []byte) (n int, err error) {
	_b := make([]byte, len(b))
	copy(_b, b)

	msg := &sarama.ProducerMessage{
		Topic: ws.topic,
		Value: sarama.ByteEncoder(_b),
	}

	ws.producer.Input() <- msg

	return len(_b), nil
}

func (ws *KafkaWriter) Sync() error {
	ws.producer.AsyncClose()
	return nil
}

func DefaultProducerConfig() sarama.Config {
	asyncProducerDefaultConfigInit.Do(func() {
		asyncProducerDefaultConfig = *sarama.NewConfig()
		asyncProducerDefaultConfig.Producer.Return.Errors = true
		asyncProducerDefaultConfig.Producer.Return.Successes = false
		asyncProducerDefaultConfig.Producer.RequiredAcks = sarama.WaitForAll
		asyncProducerDefaultConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	})

	return asyncProducerDefaultConfig
}

func NewKafkaWriter(config KafkaConfig, fail func(msg []byte, err error), success func(msg []byte)) (*KafkaWriter, error) {
	p, err := sarama.NewAsyncProducer([]string{config.Host}, &config.SaramaConfig)
	if err != nil {
		return nil, err
	}

	w := &KafkaWriter{
		producer: p,
		topic:    config.Topic,
	}

	go func() {
		for {
			select {
			case <-etfoundation.Quit():
				return
			case e := <-p.Errors():
				if fail != nil {
					val, err := e.Msg.Value.Encode()
					if err != nil {
						continue
					}

					fail(val, e.Err)
				}
			case s := <-p.Successes():
				if success != nil {
					val, err := s.Value.Encode()
					if err != nil {
						continue
					}

					success(val)
				}
			}
		}
	}()

	return w, nil
}
