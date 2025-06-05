package gkafka

type Options struct {
	ProducerMap map[string]Config         `json:"producerMap" yaml:"producerMap"`
	ConsumerMap map[string]ConsumerConfig `json:"consumerMap" yaml:"consumerMap"`
}
