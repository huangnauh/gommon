package kafka

import (
	"github.com/Shopify/sarama"
)

func InitKafkaProducer(conf *KafkaConfig) (sarama.AsyncProducer, error) {
	config, err := newConfig(conf)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewAsyncProducer(conf.Brokers, config)
	if err != nil {
		return nil, err
	}
	return producer, nil
}
