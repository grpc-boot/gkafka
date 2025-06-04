package main

import (
	"fmt"
	"time"

	"github.com/grpc-boot/gkafka"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	consumer *gkafka.Consumer
	topic    = `gkafka_test`
)

func init() {
	gkafka.SetDebugLog(func(c *gkafka.Consumer, p *gkafka.Producer, msg string, args ...any) {
		msg = fmt.Sprintf(msg, args...)
		if c != nil {
			fmt.Printf("consumer debug msg[%s]: %s", time.Now().Format(time.DateTime), msg)
		} else if p != nil {
			fmt.Printf("producer debug msg[%s]: %s", time.Now().Format(time.DateTime), msg)
		}
	})

	gkafka.SetErrorLog(func(err error, c *gkafka.Consumer, p *gkafka.Producer, msg string, args ...any) {
		msg = fmt.Sprintf(msg, args...)
		if c != nil {
			fmt.Printf("consumer error msg[%s]: %s", time.Now().Format(time.DateTime), msg)
		} else if p != nil {
			fmt.Printf("producer error msg[%s]: %s", time.Now().Format(time.DateTime), msg)
		}
	})

	confJson := `{
		"bootstrap.servers":        "127.0.0.1:39092",
		"group.id":                 "gkafka_test",
		"auto.offset.reset":        "earliest"
	}`

	consumerConf, err := gkafka.LoadJsonConf4Consumer([]byte(confJson))
	if err != nil {
		fmt.Printf("load consumer config error:%v\n", err)
		return
	}

	consumer, err = gkafka.NewConsumer(consumerConf)
	if err != nil {
		fmt.Printf("init consumer error:%v\n", err)
		return
	}
}

func main() {
	err := consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("subscribe topics error:%v\n", err)
		return
	}

	defer shutDown()

	consumer.HandlerEvent(func(c *gkafka.Consumer, event kafka.Event, done bool) {
		if e, ok := event.(*kafka.Message); ok {
			fmt.Printf("consumer msg topic:%s partition:%d key:%s value:%s offset:%d total:%d\n",
				*e.TopicPartition.Topic,
				e.TopicPartition.Partition,
				string(e.Key),
				e.Value,
				e.TopicPartition.Offset,
				c.Total(),
			)
		}
	})
}

func shutDown() {
	fmt.Println("shutdown")
}
