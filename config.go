package gkafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	defaultConsumerConfig = func() Config {
		return Config{
			//Action to take when there is no initial offset in offset store or the desired offset is out of range:
			//'smallest','earliest' - automatically reset the offset to the smallest offset
			//'largest','latest' - automatically reset the offset to the largest offset
			//'error' - trigger an error (ERR__AUTO_OFFSET_RESET) which is retrieved by consuming messages and checking 'message->err'.
			"auto.offset.reset": "earliest",
			//Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition
			"enable.partition.eof": true,
		}
	}

	defaultProducerConfig = func() Config {
		return Config{
			//Maximum number of messages allowed on the producer queue.
			//This queue is shared by all topics and partitions.
			"queue.buffering.max.messages": 1024000,
			//Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers.
			//A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
			"queue.buffering.max.ms": 1000,
		}
	}
)

type ConsumerConfig struct {
	Topics        []string `json:"topics" yaml:"topics"`
	PollTimeoutMs int      `json:"pollTimeoutMs" yaml:"pollTimeoutMs"`
	EventType     string   `json:"eventType" yaml:"eventType"`
	Config        Config   `json:"config" yaml:"config"`
}

func (cc *ConsumerConfig) GetPollTimeoutMs() int {
	if cc.PollTimeoutMs < 1 {
		return 10 * 1000
	}

	return cc.PollTimeoutMs
}

type Config map[string]any

func (c Config) WithDefaultConsumerConfig() {
	cm := defaultConsumerConfig()
	for field, value := range cm {
		if _, exists := c[field]; !exists {
			c[field] = value
		}
	}
}

func (c Config) WithDefaultProducerConfig() {
	cm := defaultProducerConfig()
	for field, value := range cm {
		if _, exists := c[field]; !exists {
			c[field] = value
		}
	}
}

func (c Config) ToKafkaConfig() *kafka.ConfigMap {
	cm := &kafka.ConfigMap{}
	for key, value := range c {
		switch val := value.(type) {
		case float64:
			_ = cm.SetKey(key, int(val))
		case int64:
			_ = cm.SetKey(key, int(val))
		default:
			_ = cm.SetKey(key, val)
		}
	}

	return cm
}

func LoadJsonConf4Consumer(data []byte) (Config, error) {
	option := defaultConsumerConfig()

	err := JsonUnmarshal(data, &option)
	if err != nil {
		return nil, err
	}

	return option, nil
}

func LoadYamlConf4Consumer(data []byte) (Config, error) {
	option := defaultConsumerConfig()

	err := YamlUnmarshal(data, &option)
	if err != nil {
		return nil, err
	}

	return option, nil
}

func LoadJsonConf4Producer(data []byte) (Config, error) {
	option := defaultProducerConfig()

	err := JsonUnmarshal(data, &option)
	if err != nil {
		return nil, err
	}

	return option, nil
}

func LoadYamlConf4Producer(data []byte) (Config, error) {
	option := defaultProducerConfig()

	err := YamlUnmarshal(data, &option)
	if err != nil {
		return nil, err
	}

	return option, nil
}
