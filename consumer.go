package gkafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/atomic"
)

type Consumer struct {
	*kafka.Consumer
	conf    *kafka.ConfigMap
	total   atomic.Int64
	groupId string
}

func NewConsumer(conf Config) (*Consumer, error) {
	cm := conf.ToKafkaConfig()
	rdConsumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		Consumer: rdConsumer,
		conf:     cm,
		groupId:  conf["group.id"].(string),
	}

	return consumer, nil
}

func (c *Consumer) GroupId() string {
	return c.groupId
}

func (c *Consumer) Conf() *kafka.ConfigMap {
	return c.conf
}

func (c *Consumer) HandlerEvent(timeoutMs int, eh ConsumerEventHandler) {
	for {
		ev := c.Poll(timeoutMs)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case kafka.AssignedPartitions:
			WriteDebug(c, nil, "assigned partitions")
			err := c.Assign(e.Partitions)
			if err != nil {
				WriteError(err, c, nil, "assigned partitions failed")
			}
		case kafka.RevokedPartitions:
			WriteDebug(c, nil, "revoked partitions")
			err := c.Unassign()
			if err != nil {
				WriteError(err, c, nil, "revoked partitions failed")
			}
		case kafka.PartitionEOF:
			WriteDebug(c, nil, "partition EOF")
		case *kafka.Message:
			c.total.Inc()
		case kafka.Error:
			WriteError(e, c, nil, "consume kafka error")
		}
		eh(c, ev)
	}
}

func (c *Consumer) Total() int64 {
	return c.total.Load()
}
