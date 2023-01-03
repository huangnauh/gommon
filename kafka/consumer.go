package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type KafkaConfig struct {
	Debug             bool     `json:"debug" toml:"debug" yaml:"debug"`
	Brokers           []string `json:"brokers" toml:"brokers" yaml:"brokers"`
	Version           string   `json:"version" toml:"version" yaml:"version"`
	Group             string   `json:"group" toml:"group" yaml:"group"`
	ClientID          string   `json:"client_id" toml:"client_id" yaml:"client_id"`
	Topic             string   `json:"topic" toml:"topic" yaml:"topic"`
	MinFetchSize      int32    `json:"min_fetch_bytes" toml:"min_fetch_bytes" yaml:"min_fetch_bytes"`
	MaxFetchSize      int32    `json:"max_fetch_bytes" toml:"max_fetch_bytes" yaml:"max_fetch_bytes"`
	ChannelBufferSize int      `json:"channel_buffer" toml:"channel_buffer" yaml:"channel_buffer"`
	FlushRetry        int      `json:"flush_retry" toml:"flush_retry" yaml:"flush_retry"`
	FlushRetryTimeout int      `json:"flush_retry_timeout" toml:"flush_retry_timeout" yaml:"flush_retry_timeout"`
	FlushFrequency    int      `json:"flush_frequency" toml:"flush_frequency" yaml:"flush_frequency"`
	DefaultFetchSize  int32    `json:"default_fetch_bytes" toml:"default_fetch_bytes" yaml:"default_fetch_bytes"`
	FetchWaitTime     int      `json:"fetch_wait_time" toml:"fetch_wait_time" yaml:"fetch_wait_time"`
	Timeout           int      `json:"timeout" toml:"timeout" yaml:"timeout"`
	KeepAlive         int      `json:"keepalive" toml:"keepalive" yaml:"keepalive"`
	CommitInterval    int      `json:"commit_interval" toml:"commit_interval" yaml:"commit_interval"`
	InitOffset        int64    `json:"init_offset" toml:"init_offset" yaml:"init_offset"`
	Retry             int      `json:"retry" toml:"retry" yaml:"retry"`
	RetrySleep        int      `json:"retry_sleep" toml:"retry_sleep" yaml:"retry_sleep"`
	Batch             int      `json:"batch" toml:"batch" yaml:"batch"`
	LimitRate         int      `json:"limit_rate" toml:"limit_rate" yaml:"limit_rate"`
	LimitBursts       int      `json:"limit_bursts" toml:"limit_bursts" yaml:"limit_bursts"`
	Workers           int      `json:"workers" toml:"workers" yaml:"workers"`
}

func DefaultConfig() KafkaConfig {
	return KafkaConfig{
		Brokers:           []string{"127.0.0.1:9092"},
		Group:             "",
		Topic:             "",
		MinFetchSize:      512,
		MaxFetchSize:      204800,
		DefaultFetchSize:  204800,
		ChannelBufferSize: 1000,
		FlushRetry:        10,
		FlushRetryTimeout: 15,
		FetchWaitTime:     100,
		Timeout:           15,
		KeepAlive:         900,
		CommitInterval:    15,
		InitOffset:        -1, // Newest: -1, Oldest: -2
		Retry:             100,
		RetrySleep:        1,
		Batch:             1,
		LimitRate:         0,
		LimitBursts:       0,
	}
}

type ConsumerHandler interface {
	Handle(context context.Context, messages []*sarama.ConsumerMessage) error
}

type Consumer struct {
	conf        *KafkaConfig
	ctx         context.Context
	limiter     *rate.Limiter
	consumed    int
	consumedMsg string
	handler     ConsumerHandler
	guard       chan struct{}
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) runLog() {
	ctx := c.ctx
	logTicker := time.NewTicker(15 * time.Second)
	var lastConsumed int
	for {
		select {
		case <-ctx.Done():
			return
		case <-logTicker.C:
			sub := float64(c.consumed - lastConsumed)
			lastConsumed = c.consumed
			logrus.Infof("total:%d, rate:%.2f, last:%s",
				c.consumed, sub/15, c.consumedMsg)
		}
	}
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logrus.Infof("Starting Consumer Group Handler, topic: %s, partition %d",
		claim.Topic(), claim.Partition())
	msgs := make([]*sarama.ConsumerMessage, 0, c.conf.Batch)
	for message := range claim.Messages() {
		if c.limiter != nil {
			if err := c.limiter.Wait(c.ctx); err != nil {
				logrus.Errorf("wait error, %s", err.Error())
			}
		}
		msgs = append(msgs, message)
		if len(msgs) >= c.conf.Batch {
			if c.guard != nil {
				c.guard <- struct{}{}
			}
			for i := 0; i < c.conf.Retry; i++ {
				err := c.handler.Handle(session.Context(), msgs)
				if err != nil {
					time.Sleep(time.Second * time.Duration(c.conf.RetrySleep))
					continue
				}
				c.consumed += len(msgs)
				lastMsg := msgs[len(msgs)-1]
				session.MarkMessage(lastMsg, "")
				c.consumedMsg = fmt.Sprintf("key %s, partition %d, offset %d",
					lastMsg.Key, lastMsg.Partition, lastMsg.Offset)
				msgs = msgs[:0]
				break
				//logrus.Debugf("Message marked: id %d, key %s, partition %d offset %d",
				//	goid.Get(), message.Key, message.Partition, message.Offset)
			}
			if c.guard != nil {
				<-c.guard
			}
		}
	}
	return nil
}

func newConsumerHandler(ctx context.Context, conf *KafkaConfig, handle ConsumerHandler) *Consumer {
	c := &Consumer{conf: conf, ctx: ctx, handler: handle}
	if conf.Batch <= 0 {
		conf.Batch = 1
	}

	if conf.Retry <= 0 {
		conf.Retry = 1
	}

	if conf.RetrySleep <= 0 {
		conf.RetrySleep = 1
	}

	if conf.Workers > 0 {
		c.guard = make(chan struct{}, conf.Workers)
	}

	if conf.LimitRate > 0 {
		c.limiter = rate.NewLimiter(rate.Limit(conf.LimitRate), conf.LimitBursts)
	}

	return c
}

func newConfig(conf *KafkaConfig) (*sarama.Config, error) {
	config := sarama.NewConfig()
	if conf.Version != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(conf.Version)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse Kafka version: %v\n", err)
		}
		config.Version = parsedVersion
	} else {
		config.Version = sarama.V2_5_0_0
	}
	if conf.ClientID != "" {
		config.ClientID = conf.ClientID
	} else if conf.Group != "" {
		config.ClientID = conf.Group
	}
	config.Consumer.Offsets.Initial = conf.InitOffset
	//config.Consumer.Offsets.CommitInterval = time.Duration(conf.CommitInterval) * time.Second
	config.Net.DialTimeout = time.Duration(conf.Timeout) * time.Second
	config.Net.ReadTimeout = time.Duration(conf.Timeout) * time.Second
	config.Net.WriteTimeout = time.Duration(conf.Timeout) * time.Second
	config.Net.KeepAlive = time.Duration(conf.KeepAlive) * time.Second
	config.Consumer.Return.Errors = true
	config.Consumer.Fetch.Min = conf.MinFetchSize
	config.Consumer.Fetch.Max = conf.MaxFetchSize
	config.Consumer.Fetch.Default = conf.DefaultFetchSize
	config.Consumer.MaxWaitTime = time.Duration(conf.FetchWaitTime) * time.Millisecond
	//config.Group.Return.Notifications = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Timeout = time.Duration(conf.Timeout) * time.Second
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = conf.FlushRetry
	config.Producer.Retry.Backoff = time.Duration(conf.FlushRetryTimeout) * time.Second
	config.Producer.Flush.Frequency = time.Duration(conf.FlushFrequency) * time.Second
	return config, nil
}

func InitConsumer(ctx context.Context, conf *KafkaConfig, handle ConsumerHandler) error {
	config, err := newConfig(conf)
	if err != nil {
		return err
	}
	consumer := newConsumerHandler(ctx, conf, handle)
	group, err := sarama.NewConsumerGroup(conf.Brokers, conf.Group, config)
	if err != nil {
		return err
	}

	go consumer.runLog()

	go func() {
		for err := range group.Errors() {
			logrus.Errorf("Error from consumer: %v", err)
		}
	}()

	go func() {
		defer group.Close()
		for {
			if err := group.Consume(ctx, []string{conf.Topic}, consumer); err != nil {
				logrus.Errorf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}
