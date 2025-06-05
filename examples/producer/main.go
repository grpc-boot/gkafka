package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/grpc-boot/gkafka"
)

var (
	producer *gkafka.Producer
	topic    = `test_file_common_fill`
	total    = 102400
	startAt  time.Time
)

func init() {
	gkafka.SetDebugLog(func(c *gkafka.Consumer, p *gkafka.Producer, msg string, args ...any) {
		msg = fmt.Sprintf(msg, args...)
		if c != nil {
			fmt.Printf("consumer debug msg[%s]: %s\n", time.Now().Format(time.DateTime), msg)
		} else if p != nil {
			fmt.Printf("producer debug msg[%s]: %s\n", time.Now().Format(time.DateTime), msg)
		}
	})

	gkafka.SetErrorLog(func(err error, c *gkafka.Consumer, p *gkafka.Producer, msg string, args ...any) {
		msg = fmt.Sprintf(msg, args...)
		if c != nil {
			fmt.Printf("consumer error msg[%s]: %s\n", time.Now().Format(time.DateTime), msg)
		} else if p != nil {
			fmt.Printf("producer error msg[%s]: %s\n", time.Now().Format(time.DateTime), msg)
		}
	})

	if os.Getenv("BS") == "" {
		_ = os.Setenv("BS", "127.0.0.1:39092")
	}

	confJson := fmt.Sprintf(`{
		"bootstrap.servers":   "%s",
		"batch.num.messages":           1024
	}`, os.Getenv("BS"))

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
		var (
			sha1Arr = sha1.Sum(gkafka.String2Bytes(strconv.FormatInt(current+int64(start), 10)))
			id      = hex.EncodeToString(sha1Arr[:])
		)

		/*event := gkafka.AcquireEventWithId(id, "new file common").WithData(gkafka.Param{
			"file_hash": id,
		}).WithTag("rs", "xt")
		_ = producer.ProduceMsgAsync(topic, event.JsonMarshal())*/

		event := gkafka.AcquireEventWithId(id, "new file custom").WithData(gkafka.Param{
			"file_hash": id,
		}).WithTag("rs", "xt")
		_ = producer.ProduceMsgAsync(topic, event.YamlMarshal())
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
