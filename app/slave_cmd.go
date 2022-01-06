package app

const (
	CHAN_SIZE = 512
)

var (
	slaveMsgChan = make(chan []byte, CHAN_SIZE)
)

// msg:  nodeid:addr:port
func SlaveHandler(req []byte, rsp *[]byte) {

}
