package gkafka

import (
	"math"
	"sync"
	"time"
)

var (
	eventPool = sync.Pool{
		New: func() interface{} {
			e := &Event{}
			e.reset()
			return e
		},
	}

	AcquireEvent = func() *Event {
		ev := eventPool.Get().(*Event)
		ev.UnixTime = time.Now().Unix()
		ev.index = -1
		ev.middlewares = nil
		return ev
	}

	ReleaseEvent = func(event *Event) {
		event.reset()
		eventPool.Put(event)
	}
)

type Event struct {
	Id          string   `json:"id" yaml:"id"`
	UnixTime    int64    `json:"unixTime" yaml:"unixTime"`
	Event       string   `json:"event" yaml:"event"`
	Tags        []string `json:"tags" yaml:"tags"`
	Data        Param    `json:"data" yaml:"data"`
	index       int8
	middlewares []Handler
}

func AcquireEventWithId(id, event string) *Event {
	ev := AcquireEvent()
	ev.Id = id
	ev.Event = event
	return ev
}

func (e *Event) WithTag(tagList ...string) *Event {
	e.Tags = tagList
	return e
}

func (e *Event) HasTag(tag string) bool {
	if len(e.Tags) < 1 {
		return false
	}

	for _, t := range e.Tags {
		if t == tag {
			return true
		}
	}

	return false
}

func (e *Event) WithData(data Param) *Event {
	e.Data = data
	return e
}

func (e *Event) JsonMarshal() []byte {
	data, _ := JsonMarshal(e)
	return data
}

func (e *Event) YamlMarshal() []byte {
	data, _ := YamlMarshal(e)
	return data
}

func (e *Event) Next(topic string) error {
	e.index++
	if int(e.index) < len(e.middlewares) {
		return e.middlewares[e.index](topic, e)
	}

	return nil
}

func (e *Event) Abort() {
	e.index = math.MaxInt8
}

func (e *Event) IsAbort() bool {
	return e.index == math.MaxInt8
}

func (e *Event) JsonUnmarshal(data []byte) error {
	return JsonUnmarshal(data, e)
}

func (e *Event) reset() {
	if e.Tags == nil {
		e.Tags = make([]string, 0)
	} else {
		e.Tags = e.Tags[:0]
	}

	e.middlewares = nil
	e.index = -1
	e.Id = ""
	e.UnixTime = 0
	e.Event = ""
	e.Data = make(Param)
}
