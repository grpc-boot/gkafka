package gkafka

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type ConsumerEventHandler func(c *Consumer, event kafka.Event)
type ProducerEventHandler func(p *Producer, event kafka.Event, done bool)

// DefaultProduceEventHandler 默认生产者事件处理
func DefaultProduceEventHandler(p *Producer, event kafka.Event, done bool) {}
