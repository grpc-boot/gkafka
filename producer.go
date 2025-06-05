package gkafka

import (
	"context"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/atomic"
)

type Producer struct {
	*kafka.Producer
	conf         *kafka.ConfigMap
	successTotal atomic.Int64
	failedTotal  atomic.Int64
}

func NewProducer(conf Config, eh ProducerEventHandler) (*Producer, error) {
	cm := conf.ToKafkaConfig()
	rdProducer, err := kafka.NewProducer(cm)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		Producer: rdProducer,
		conf:     cm,
	}

	if eh == nil {
		eh = DefaultProduceEventHandler
	}

	go producer.handlerEvent(eh)
	return producer, nil
}

func (p *Producer) handlerEvent(handler ProducerEventHandler) {
	for {
		e, ok := <-p.Events()
		if !ok {
			handler(p, e, true)
			return
		}

		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				failedTotal := p.failedTotal.Inc()
				WriteError(m.TopicPartition.Error, nil, p, "delivery msg to [topic:%s] [partition: %d] with [key: %s] failed [fail total: %d]: %s", *m.TopicPartition.Topic, m.TopicPartition.Partition, Bytes2String(m.Key), failedTotal, Bytes2String(m.Value))
			} else {
				okTotal := p.successTotal.Inc()
				WriteDebug(nil, p, "delivered msg to [topic:%s] [partition: %d] with [key: %s] success [success total: %d]: %s", *m.TopicPartition.Topic, m.TopicPartition.Partition, Bytes2String(m.Key), okTotal, Bytes2String(m.Value))
			}
		case kafka.Error:
			WriteError(ev, nil, p, "event error")
		default:
			WriteDebug(nil, p, "ignored event")
		}
		handler(p, e, false)
	}
}

func (p *Producer) id2Key(id int64) []byte {
	return []byte(strconv.FormatInt(id, 10))
}

func (p *Producer) Conf() *kafka.ConfigMap {
	return p.conf
}

// ProduceAsync 向librdkafka写消息，错误类型包括：队列已满、超时等
func (p *Producer) ProduceAsync(topic string, value []byte, partition int32, key []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
		},
		Value: value,
		Key:   key,
	}

	WriteDebug(nil, p, "send msg to [topic:%s] [partition: %d] buffer with [key: %s]: %s", topic, partition, key, value)
	return p.Produce(msg, nil)
}

// ProduceMsgAsync 向librdkafka发送消息，错误类型包括：队列已满、超时等
func (p *Producer) ProduceMsgAsync(topic string, value []byte) error {
	return p.ProduceAsync(topic, value, kafka.PartitionAny, nil)
}

// ProduceOrderAsync 向librdkafka顺序(同一个id会发送到同一个partition)发送消息
func (p *Producer) ProduceOrderAsync(topic string, value []byte, id int64) error {
	return p.ProduceAsync(topic, value, kafka.PartitionAny, p.id2Key(id))
}

func (p *Producer) FlushAll(ctx context.Context) (err error) {
	select {
	default:
	case <-ctx.Done():
		err = ctx.Err()
		WriteError(err, nil, p, "producer flush all failed")
		return
	}

	WriteDebug(nil, p, "producer begin flush all")

	rest := 1
	for rest > 0 {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			WriteError(err, nil, p, "producer flush all failed")
			return
		default:
			rest = p.Flush(500)
		}
	}

	WriteDebug(nil, p, "producer flush all done")
	return nil
}

func (p *Producer) TotalSuccess() int64 {
	return p.successTotal.Load()
}

func (p *Producer) TotalFailed() int64 {
	return p.failedTotal.Load()
}

func (p *Producer) SuccessRate() float64 {
	total := p.TotalSuccess() + p.TotalFailed()
	if total == 0 {
		return 1
	}

	return float64(p.TotalSuccess()) / float64(total)
}
