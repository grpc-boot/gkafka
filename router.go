package gkafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Handler func(topic string, e *Event) error
type EventUnmarshal func(msg *kafka.Message, en *Event) error

var (
	DefaultEventUnmarshal = func(eventType string) EventUnmarshal {
		switch eventType {
		case "yml", "yaml":
			return func(msg *kafka.Message, en *Event) error {
				return YamlUnmarshal(msg.Value, en)
			}
		default:
			return func(msg *kafka.Message, en *Event) error {
				return JsonUnmarshal(msg.Value, en)
			}
		}
	}
)

type Router struct {
	handlers       map[string]Handler
	euSelector     func(eventType string) EventUnmarshal
	errHandler     func(err error, topic string, e *Event)
	recoverHandler func(err error, topic string, e *Event)
	middlewares    []Handler
}

func NewRouter() *Router {
	return &Router{
		handlers:    make(map[string]Handler),
		middlewares: make([]Handler, 0),
		euSelector:  DefaultEventUnmarshal,
	}
}

func (r *Router) SetErrorHandler(eh func(err error, topic string, e *Event)) *Router {
	r.errHandler = eh
	return r
}

func (r *Router) SetRecoverHandler(rh func(err error, topic string, e *Event)) *Router {
	r.recoverHandler = rh
	return r
}

func (r *Router) Use(middleware ...Handler) *Router {
	r.middlewares = append(r.middlewares, middleware...)
	return r
}

func (r *Router) BindHandler(event string, handler Handler) *Router {
	r.handlers[event] = handler
	return r
}

func (r *Router) SetEventUnmarshal(euSelector func(eventType string) EventUnmarshal) {
	r.euSelector = euSelector
}

func (r *Router) Handler(eu EventUnmarshal) ConsumerEventHandler {
	return func(c *Consumer, event kafka.Event) {
		if e, ok := event.(*kafka.Message); ok {
			var (
				topic = *e.TopicPartition.Topic
				en    = AcquireEvent()
			)

			defer func() {
				if er := recover(); er != nil {
					var err error
					switch er.(type) {
					default:
						err = fmt.Errorf("panic:%v", er)
					case error:
						err = er.(error)
					}

					if r.recoverHandler != nil {
						r.recoverHandler(err, topic, en)
					} else {
						WriteError(
							err,
							c,
							nil,
							"handler panic [topic:%s] [partition: %d] with [key: %s] event error: %s",
							topic,
							e.TopicPartition.Partition,
							Bytes2String(e.Key),
							Bytes2String(e.Value),
						)
					}
				}

				ReleaseEvent(en)
			}()

			err := eu(e, en)
			if err != nil {
				WriteError(
					err,
					c,
					nil,
					"unmarshal [topic:%s] [partition: %d] with [key: %s] msg failed: %s",
					topic,
					e.TopicPartition.Partition,
					Bytes2String(e.Key),
					Bytes2String(e.Value),
				)
				return
			}

			if h, exists := r.handlers[en.Event]; exists {
				en.index = -1
				en.middlewares = append(r.middlewares, h)

				if err = en.Next(topic); err != nil {
					if r.errHandler != nil {
						r.errHandler(err, topic, en)
					} else {
						WriteError(
							err,
							c,
							nil,
							"handler [topic:%s] [partition: %d] with [key: %s] event error: %s",
							topic,
							e.TopicPartition.Partition,
							Bytes2String(e.Key),
							Bytes2String(e.Value),
						)
					}
				}
			}
		}
	}
}
