package gkafka

var (
	debugLog DebugLog
	errorLog ErrLog
)

type (
	DebugLog func(c *Consumer, p *Producer, msg string, args ...any)
	ErrLog   func(err error, c *Consumer, p *Producer, msg string, args ...any)
)

func SetErrorLog(l ErrLog) {
	errorLog = l
}

func SetDebugLog(d DebugLog) {
	debugLog = d
}

func WriteDebug(c *Consumer, p *Producer, msg string, args ...any) {
	if debugLog == nil {
		return
	}

	debugLog(c, p, msg, args...)
}

func WriteError(err error, c *Consumer, p *Producer, msg string, args ...any) {
	if errorLog == nil || err == nil {
		return
	}

	errorLog(err, c, p, msg, args...)
}
