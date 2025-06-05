package gkafka

import (
	"context"
	"sync"
	"unsafe"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/goccy/go-json"
	"gopkg.in/yaml.v3"
)

var (
	JsonMarshal   = json.Marshal
	JsonUnmarshal = json.Unmarshal
	YamlMarshal   = yaml.Marshal
	YamlUnmarshal = yaml.Unmarshal
)

func InitContainer(opts *Options, r *Router, ph ProducerEventHandler, rb kafka.RebalanceCb) error {
	if opts == nil {
		return nil
	}

	if len(opts.ProducerMap) > 0 {
		for key, conf := range opts.ProducerMap {
			conf.WithDefaultProducerConfig()
			p, err := NewProducer(conf, ph)
			if err != nil {
				return err
			}

			PutProducer(key, p)
		}
	}

	if len(opts.ConsumerMap) > 0 {
		for key, conf := range opts.ConsumerMap {
			conf.Config.WithDefaultConsumerConfig()
			c, err := NewConsumer(conf.Config)
			if err != nil {
				return err
			}

			err = c.SubscribeTopics(conf.Topics, rb)
			if err != nil {
				return err
			}

			go c.HandlerEvent(conf.PollTimeoutMs, r.Handler(r.euSelector(conf.EventType)))

			PutConsumer(key, c)
		}
	}

	return nil
}

func CloseAll(ctx context.Context) {
	var (
		consumers = AllConsumers()
		producers = AllProducers()
		size      = len(consumers) + len(producers)
	)

	if size == 0 {
		return
	}

	wa := &sync.WaitGroup{}
	wa.Add(size)

	if len(consumers) > 0 {
		for _, consumer := range consumers {
			go func(c *Consumer) {
				if c == nil {
					wa.Done()
					return
				}

				select {
				default:
					_ = c.Close()
				case <-ctx.Done():
				}
				wa.Done()
			}(consumer)
		}
	}

	if len(producers) > 0 {
		for _, producer := range producers {
			go func(p *Producer) {
				if p == nil {
					wa.Done()
					return
				}

				_ = p.FlushAll(ctx)
				p.Close()
				wa.Done()
			}(producer)
		}
	}

	wa.Wait()
}

// Bytes2String converts byte slice to string.
func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// String2Bytes converts string to byte slice.
func String2Bytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
