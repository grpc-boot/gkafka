package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/grpc-boot/gkafka"
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
			fmt.Printf("consumer error msg[%s]: %s error: %v\n", time.Now().Format(time.DateTime), msg, err)
		} else if p != nil {
			fmt.Printf("producer error msg[%s]: %s error: %v\n", time.Now().Format(time.DateTime), msg, err)
		}
	})
}

func main() {
	var (
		opt = &gkafka.Options{}
		err = gkafka.JsonUnmarshal([]byte(`{
          "producerMap": {
			 "file_common_fill": {
                "bootstrap.servers": "127.0.0.1:39092",
				"batch.num.messages":           1024
             }
          },
          "consumerMap": {
             "file_common_fill": {
                "topics": ["test_file_common_fill"],
				"eventType": "yml",
				"config": {
                  "bootstrap.servers": "127.0.0.1:39092",
                  "group.id":                 "test_file_common_fill",
                  "auto.offset.reset":        "earliest"
                }
             }
          }
        }`), opt)
	)

	if err != nil {
		fmt.Printf("json unmarshal fail: %s", err)
	}

	r := gkafka.NewRouter()

	r.Use(func(topic string, e *gkafka.Event) error {
		fmt.Printf("mid1 start \n")

		err = e.Next(topic)

		fmt.Printf("mid1 end \n")
		return err
	})

	r.Use(func(topic string, e *gkafka.Event) error {
		fmt.Printf("mid2 start \n")

		err = e.Next(topic)

		fmt.Printf("mid2 end \n")
		return err
	})

	r.BindHandler("new file custom", func(topic string, e *gkafka.Event) error {
		fmt.Printf("new file custom topic:%s msg: %s\n", topic, gkafka.Bytes2String(e.JsonMarshal()))
		return nil
	})

	r.BindHandler("new file", func(topic string, e *gkafka.Event) error {
		fmt.Printf("new file topic:%s msg: %s\n", topic, gkafka.Bytes2String(e.JsonMarshal()))
		return nil
	})

	err = gkafka.InitContainer(opt, r, nil, nil)
	if err != nil {
		fmt.Printf("InitContainer fail: %s", err)
	}

	go func() {
		var (
			p      = gkafka.GetProducer("file_common_fill")
			ticker = time.NewTicker(time.Second * 3)
			num    int
		)

		for range ticker.C {
			var (
				id  = sha1.Sum([]byte(strconv.Itoa(num)))
				key = hex.EncodeToString(id[:])
				ev  *gkafka.Event
			)

			if num%2 == 0 {
				ev = gkafka.AcquireEventWithId(key, "new file custom").WithData(gkafka.Param{
					"id": key,
				})
			} else {
				ev = gkafka.AcquireEventWithId(key, "new file").WithData(gkafka.Param{
					"id": key,
				})
			}

			err = p.ProduceOrderAsync("test_file_common_fill", ev.YamlMarshal(), int64(num))
			if err != nil {
				fmt.Printf("ProduceOrderAsync fail: %s", err)
			}
			gkafka.ReleaseEvent(ev)
			num++
		}

	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		gkafka.CloseAll(ctx)
		cancel()
	}()

	done := make(chan struct{})
	<-done
}
