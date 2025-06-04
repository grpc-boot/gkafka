package gkafka

import (
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
		return ev
	}

	ReleaseEvent = func(event *Event) {
		event.reset()
		eventPool.Put(event)
	}
)

type Event struct {
	Id       string   `json:"id"`
	UnixTime int64    `json:"unixTime"`
	Event    string   `json:"event"`
	Tags     []string `json:"tags"`
	Data     Param    `json:"data"`
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

func (e *Event) JsonUnmarshal(data []byte) error {
	return JsonUnmarshal(data, e)
}

func (e *Event) reset() {
	if e.Tags == nil {
		e.Tags = make([]string, 0)
	} else {
		e.Tags = e.Tags[:0]
	}

	e.Id = ""
	e.UnixTime = 0
	e.Event = ""
	e.Data = make(Param)
}
