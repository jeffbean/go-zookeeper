package proto

import (
	"reflect"
	"testing"
)

func TestEncodeDecodePacket(t *testing.T) {
	t.Parallel()
	encodeDecodeTest(t, &RequestHeader{-2, 5})
	encodeDecodeTest(t, &ConnectResponse{1, 2, 3, nil})
	encodeDecodeTest(t, &ConnectResponse{1, 2, 3, []byte{4, 5, 6}})
	encodeDecodeTest(t, &GetAclResponse{[]ACL{{12, "s", "anyone"}}, Stat{}})
	encodeDecodeTest(t, &GetChildrenResponse{[]string{"foo", "bar"}})
	encodeDecodeTest(t, &pathWatchRequest{"path", true})
	encodeDecodeTest(t, &pathWatchRequest{"path", false})
	encodeDecodeTest(t, &CheckVersionRequest{"/", -1})
	encodeDecodeTest(t, &MultiRequest{Ops: []MultiRequestOp{{MultiHeader{opCheck, false, -1}, &CheckVersionRequest{"/", -1}}}})
}

func TestRequestStructForOp(t *testing.T) {
	for op, name := range opNames {
		if op != OpNotify && op != OpWatcherEvent {
			if s := RequestStructForOp(op); s == nil {
				t.Errorf("No struct for op %s", name)
			}
		}
	}
}

func encodeDecodeTest(t *testing.T, r interface{}) {
	buf := make([]byte, 1024)
	n, err := EncodePacket(buf, r)
	if err != nil {
		t.Errorf("encodePacket returned non-nil error %+v\n", err)
		return
	}
	t.Logf("%+v %x", r, buf[:n])
	r2 := reflect.New(reflect.ValueOf(r).Elem().Type()).Interface()
	n2, err := DecodePacket(buf[:n], r2)
	if err != nil {
		t.Errorf("decodePacket returned non-nil error %+v\n", err)
		return
	}
	if n != n2 {
		t.Errorf("sizes don't match: %d != %d", n, n2)
		return
	}
	if !reflect.DeepEqual(r, r2) {
		t.Errorf("results don't match: %+v != %+v", r, r2)
		return
	}
}

func TestEncodeShortBuffer(t *testing.T) {
	t.Parallel()
	_, err := EncodePacket([]byte{}, &RequestHeader{1, 2})
	if err != ErrShortBuffer {
		t.Errorf("encodePacket should return ErrShortBuffer on a short buffer instead of '%+v'", err)
		return
	}
}

func TestDecodeShortBuffer(t *testing.T) {
	t.Parallel()
	_, err := DecodePacket([]byte{}, &ResponseHeader{})
	if err != ErrShortBuffer {
		t.Errorf("decodePacket should return ErrShortBuffer on a short buffer instead of '%+v'", err)
		return
	}
}

func BenchmarkEncode(b *testing.B) {
	buf := make([]byte, 4096)
	st := &ConnectRequest{Passwd: []byte("1234567890")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := encodePacket(buf, st); err != nil {
			b.Fatal(err)
		}
	}
}
