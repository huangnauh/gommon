package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type ConsumerConfig struct {
	Debug            bool     `json:"debug" toml:"debug"`
	Brokers          []string `json:"brokers" toml:"brokers"`
	Group            string   `json:"group" toml:"group"`
	Topic            string   `json:"topic" toml:"topic"`
	MinFetchSize     int32    `json:"min_fetch_bytes" toml:"min_fetch_bytes"`
	MaxFetchSize     int32    `json:"max_fetch_bytes" toml:"max_fetch_bytes"`
	DefaultFetchSize int32    `json:"default_fetch_bytes" toml:"default_fetch_bytes"`
	FetchWaitTime    int      `json:"fetch_wait_time" toml:"fetch_wait_time"`
	Timeout          int      `json:"timeout" toml:"timeout"`
	KeepAlive        int      `json:"keepalive" toml:"keepalive"`
	CommitInterval   int      `json:"commit_interval" toml:"commit_interval"`
	InitOffset       int64    `json:"init_offset" toml:"init_offset"`
	Retry            int      `json:"retry" toml:"retry"`
	RetrySleep       int      `json:"retry_sleep" toml:"retry_sleep"`
	Batch            int      `json:"batch" toml:"batch"`
	LimitRate        int      `json:"limit_rate" toml:"limit_rate"`
	LimitBursts      int      `json:"limit_bursts" toml:"limit_bursts"`
	Workers          int      `json:"workers" toml:"workers"`
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Brokers:          []string{"127.0.0.1:9092"},
		Group:            "",
		Topic:            "",
		MinFetchSize:     512,
		MaxFetchSize:     204800,
		DefaultFetchSize: 204800,
		FetchWaitTime:    100,
		Timeout:          15,
		KeepAlive:        900,
		CommitInterval:   15,
		InitOffset:       -1, // Newest: -1, Oldest: -2
		Retry:            100,
		RetrySleep:       1,
		Batch:            1,
		LimitRate:        0,
		LimitBursts:      0,
	}
}

type ConsumerHandler interface {
	Handle(context context.Context, messages []*sarama.ConsumerMessage) error
}

type Consumer struct {
	conf        *ConsumerConfig
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

func newConsumerHandler(ctx context.Context, conf *ConsumerConfig, handle ConsumerHandler) *Consumer {
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

func newConsumerConfig(conf *ConsumerConfig) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.ClientID = conf.Group
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
	return config
}

func InitConsumer(ctx context.Context, conf *ConsumerConfig, handle ConsumerHandler) error {
	config := newConsumerConfig(conf)
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
