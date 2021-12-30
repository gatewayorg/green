package codec

import "testing"

func TestExtractKey(t *testing.T) {
	src := []byte("*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
	key, err := ExtactKey(src)
	t.Logf("key: %s err: %v \n", key, err)
}
