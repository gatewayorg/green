package app

type KVFunc func(req []byte, rsp *[]byte)

var cmdMap = map[string]KVFunc{
	// for keys
	"DEL":    DelHandler,
	"EXISTS": nil,
	// for strings
	"SET": SetHandler,
	"GET": GetHandler,
	// for Pub/Sub
	"PSUBSCRIBE":   nil,
	"PUBSUB":       nil,
	"PUBLISH":      nil,
	"PUNSUBSCRIBE": nil,
	"SUBSCRIBE":    nil,
	"UNSUBSCRIBE":  nil,
	// for admin
	"PING": PingHandler,
	// for cluster
	"SLAVEOF": SlaveHandler,
	"SYNC":    SyncHandler,
}

var (
	ErrNotSupportCommand = "- not support command\r\n"
)
