package main

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-boot/gkafka"
)

var (
	producer *gkafka.Producer
	topic    = `gkafka_test`
	total    = 102400
	startAt  time.Time
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
		"bootstrap.servers":   "127.0.0.1:39092",
		"batch.num.messages":           1024
	}`

	producerConf, err := gkafka.LoadJsonConf4Producer([]byte(confJson))
	if err != nil {
		fmt.Printf("load gkafka producer conf error: %v\n", err)
		return
	}

	producer, err = gkafka.NewProducer(producerConf, gkafka.DefaultProduceEventHandler)
	if err != nil {
		fmt.Printf("init producer error:%v\n", err)
		return
	}
}

func main() {
	defer func() {
		producer.Close()
		shutDown()
	}()

	startAt = time.Now()
	current := startAt.UnixNano()
	for start := 0; start < total; start++ {
		value, _ := gkafka.JsonMarshal(map[string]interface{}{
			"Id": current + int64(start),
		})
		producer.ProduceMsgAsync(topic, value)
		//producer.ProduceOrderAsync(topic, value, 1657780437524912117)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err := producer.FlushAll(ctx)
	if err != nil {
		fmt.Printf("flush msg error:%v\n", err)
	}

	fmt.Printf("successTotal:%d, failedTotal:%d successRate:%.4f\n", producer.TotalSuccess(), producer.TotalFailed(), producer.SuccessRate())
}

func shutDown() {
	fmt.Printf("produce %d cost:%s\n", total, time.Since(startAt))
}
