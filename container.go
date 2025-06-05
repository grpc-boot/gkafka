package gkafka

import (
	"fmt"
	"strings"
	"sync"
)

const (
	producerPrefix = `p:`
	consumerPrefix = `c:`
)

var (
	container sync.Map
)

func producerKey(key string) string {
	return fmt.Sprintf("%s%s", producerPrefix, key)
}

func consumerKey(key string) string {
	return fmt.Sprintf("%s%s", consumerPrefix, key)
}

func AllProducers() map[string]*Producer {
	ps := make(map[string]*Producer)

	container.Range(func(key, value any) bool {
		if cKey, _ := key.(string); cKey != "" && strings.HasPrefix(cKey, producerPrefix) {
			ps[cKey], _ = value.(*Producer)
		}
		return true
	})
	return ps
}

func AllConsumers() map[string]*Consumer {
	ps := make(map[string]*Consumer)

	container.Range(func(key, value any) bool {
		if cKey, _ := key.(string); cKey != "" && strings.HasPrefix(cKey, consumerPrefix) {
			ps[cKey], _ = value.(*Consumer)
		}
		return true
	})
	return ps
}

func PutProducer(key string, producer *Producer) {
	container.Store(producerKey(key), producer)
}

func PutConsumer(key string, consumer *Consumer) {
	container.Store(consumerKey(key), consumer)
}

func GetProducer(key string) *Producer {
	value, exists := container.Load(producerKey(key))
	if !exists {
		return nil
	}

	p, _ := value.(*Producer)
	return p
}

func GetConsumer(key string) *Consumer {
	value, exists := container.Load(consumerKey(key))
	if !exists {
		return nil
	}

	c, _ := value.(*Consumer)
	return c
}
