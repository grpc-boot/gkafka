package gkafka

import (
	"github.com/goccy/go-json"
	"gopkg.in/yaml.v3"
)

var (
	JsonMarshal   = json.Marshal
	JsonUnmarshal = json.Unmarshal
	YamlMarshal   = yaml.Marshal
	YamlUnmarshal = yaml.Unmarshal
)
