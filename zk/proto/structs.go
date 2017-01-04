package proto

import (
	"encoding/binary"
	"errors"
	"reflect"
	"runtime"
)

var (
	errUnhandledFieldType = errors.New("zk: unhandled field type")
	errPtrExpected        = errors.New("zk: encode/decode expect a non-nil pointer to struct")
	errShortBuffer        = errors.New("zk: buffer too small")
)

// Stat is the ZK meta information on a znode
type Stat struct {
	Czxid          int64 // The zxid of the change that caused this znode to be created.
	Mzxid          int64 // The zxid of the change that last modified this znode.
	Ctime          int64 // The time in milliseconds from epoch when this znode was created.
	Mtime          int64 // The time in milliseconds from epoch when this znode was last modified.
	Version        int32 // The number of changes to the data of this znode.
	Cversion       int32 // The number of changes to the children of this znode.
	Aversion       int32 // The number of changes to the ACL of this znode.
	EphemeralOwner int64 // The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
	DataLength     int32 // The length of the data field of this znode.
	NumChildren    int32 // The number of children of this znode.
	Pzxid          int64 // last modified children
}

// ACL layout of the acl data of a znode
type ACL struct {
	Perms  int32
	Scheme string
	ID     string
}

// RequestHeader is the first bytes after the payload length on a client packet
type RequestHeader struct {
	Xid    int32
	Opcode int32
}

// ResponseHeader is the first 12 bits (4,8,4) on the server responce packet
type ResponseHeader struct {
	Xid  int32
	Zxid int64
	Err  ErrCode
}

type auth struct {
	Type   int32
	Scheme string
	Auth   []byte
}

// Generic request structs

type pathRequest struct {
	Path string
}

type pathVersionRequest struct {
	Path    string
	Version int32
}

type pathWatchRequest struct {
	Path  string
	Watch bool
}

type pathResponse struct {
	Path string
}

type statResponse struct {
	Stat Stat
}

//

type CheckVersionRequest pathVersionRequest
type CloseRequest struct{}
type CloseResponse struct{}

type ConnectRequest struct {
	ProtocolVersion int32
	LastZxidSeen    int64
	TimeOut         int32
	SessionID       int64
	Passwd          []byte
}

type ConnectResponse struct {
	ProtocolVersion int32
	TimeOut         int32
	SessionID       int64
	Passwd          []byte
}

type CreateRequest struct {
	Path  string
	Data  []byte
	Acl   []ACL
	Flags int32
}

type CreateResponse pathResponse
type DeleteRequest pathVersionRequest
type DeleteResponse struct{}

type errorResponse struct {
	Err int32
}

type ExistsRequest pathWatchRequest
type ExistsResponse statResponse
type GetAclRequest pathRequest

type GetAclResponse struct {
	Acl  []ACL
	Stat Stat
}

type GetChildrenRequest pathRequest

type GetChildrenResponse struct {
	Children []string
}

type GetChildren2Request pathWatchRequest

type GetChildren2Response struct {
	Children []string
	Stat     Stat
}

type GetDataRequest pathWatchRequest

type GetDataResponse struct {
	Data []byte
	Stat Stat
}

type GetMaxChildrenRequest pathRequest

type GetMaxChildrenResponse struct {
	Max int32
}

type GetSaslRequest struct {
	Token []byte
}

type PingRequest struct{}
type PingResponse struct{}

type SetAclRequest struct {
	Path    string
	Acl     []ACL
	Version int32
}

type SetAclResponse statResponse

type SetDataRequest struct {
	Path    string
	Data    []byte
	Version int32
}

type SetDataResponse statResponse

type SetMaxChildren struct {
	Path string
	Max  int32
}

type SetSaslRequest struct {
	Token string
}

type SetSaslResponse struct {
	Token string
}

type SetWatchesRequest struct {
	RelativeZxid int64
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}

type setWatchesResponse struct{}

type SyncRequest pathRequest
type SyncResponse pathResponse

type SetAuthRequest auth
type SetAuthResponse struct{}

type MultiHeader struct {
	Type int32
	Done bool
	Err  ErrCode
}

type MultiRequestOp struct {
	Header MultiHeader
	Op     interface{}
}

type MultiRequest struct {
	Ops        []MultiRequestOp
	DoneHeader MultiHeader
}

type MultiResponseOp struct {
	Header MultiHeader
	String string
	Stat   *Stat
	Err    ErrCode
}

type MultiResponse struct {
	Ops        []MultiResponseOp
	DoneHeader MultiHeader
}

func (r *MultiRequest) Encode(buf []byte) (int, error) {
	total := 0
	for _, op := range r.Ops {
		op.Header.Done = false
		n, err := encodePacketValue(buf[total:], reflect.ValueOf(op))
		if err != nil {
			return total, err
		}
		total += n
	}
	r.DoneHeader.Done = true
	n, err := encodePacketValue(buf[total:], reflect.ValueOf(r.DoneHeader))
	if err != nil {
		return total, err
	}
	total += n

	return total, nil
}

func (r *MultiRequest) Decode(buf []byte) (int, error) {
	r.Ops = make([]MultiRequestOp, 0)
	r.DoneHeader = MultiHeader{-1, true, -1}
	total := 0
	for {
		header := &MultiHeader{}
		n, err := decodePacketValue(buf[total:], reflect.ValueOf(header))
		if err != nil {
			return total, err
		}
		total += n
		if header.Done {
			r.DoneHeader = *header
			break
		}

		req := RequestStructForOp(header.Type)
		if req == nil {
			return total, ErrAPIError
		}
		n, err = decodePacketValue(buf[total:], reflect.ValueOf(req))
		if err != nil {
			return total, err
		}
		total += n
		r.Ops = append(r.Ops, MultiRequestOp{*header, req})
	}
	return total, nil
}

func (r *MultiResponse) Decode(buf []byte) (int, error) {
	var multiErr error

	r.Ops = make([]MultiResponseOp, 0)
	r.DoneHeader = MultiHeader{-1, true, -1}
	total := 0
	for {
		header := &MultiHeader{}
		n, err := decodePacketValue(buf[total:], reflect.ValueOf(header))
		if err != nil {
			return total, err
		}
		total += n
		if header.Done {
			r.DoneHeader = *header
			break
		}

		res := MultiResponseOp{Header: *header}
		var w reflect.Value
		switch header.Type {
		default:
			return total, ErrAPIError
		case OpError:
			w = reflect.ValueOf(&res.Err)
		case OpCreate:
			w = reflect.ValueOf(&res.String)
		case OpSetData:
			res.Stat = new(Stat)
			w = reflect.ValueOf(res.Stat)
		case OpCheck, OpDelete:
		}
		if w.IsValid() {
			n, err := decodePacketValue(buf[total:], w)
			if err != nil {
				return total, err
			}
			total += n
		}
		r.Ops = append(r.Ops, res)
		if multiErr == nil && res.Err != errOk {
			// Use the first error as the error returned from Multi().
			multiErr = res.Err.toError()
		}
	}
	return total, multiErr
}

type WatcherEvent struct {
	Type  EventType
	State State
	Path  string
}

type decoder interface {
	Decode(buf []byte) (int, error)
}

type encoder interface {
	Encode(buf []byte) (int, error)
}

// DecodePacket decodes the zookeeper application payload for a packet
func DecodePacket(buf []byte, st interface{}) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(runtime.Error); ok && e.Error() == "runtime error: slice bounds out of range" {
				err = errShortBuffer
			} else {
				panic(r)
			}
		}
	}()

	v := reflect.ValueOf(st)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0, errPtrExpected
	}
	return decodePacketValue(buf, v)
}

func decodePacketValue(buf []byte, v reflect.Value) (int, error) {
	rv := v
	kind := v.Kind()
	if kind == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
		kind = v.Kind()
	}

	n := 0
	switch kind {
	default:
		return n, errUnhandledFieldType
	case reflect.Struct:
		if de, ok := rv.Interface().(decoder); ok {
			return de.Decode(buf)
		} else if de, ok := v.Interface().(decoder); ok {
			return de.Decode(buf)
		} else {
			for i := 0; i < v.NumField(); i++ {
				field := v.Field(i)
				n2, err := decodePacketValue(buf[n:], field)
				n += n2
				if err != nil {
					return n, err
				}
			}
		}
	case reflect.Bool:
		v.SetBool(buf[n] != 0)
		n++
	case reflect.Int32:
		v.SetInt(int64(binary.BigEndian.Uint32(buf[n : n+4])))
		n += 4
	case reflect.Int64:
		v.SetInt(int64(binary.BigEndian.Uint64(buf[n : n+8])))
		n += 8
	case reflect.String:
		ln := int(binary.BigEndian.Uint32(buf[n : n+4]))
		v.SetString(string(buf[n+4 : n+4+ln]))
		n += 4 + ln
	case reflect.Slice:
		switch v.Type().Elem().Kind() {
		default:
			count := int(binary.BigEndian.Uint32(buf[n : n+4]))
			n += 4
			values := reflect.MakeSlice(v.Type(), count, count)
			v.Set(values)
			for i := 0; i < count; i++ {
				n2, err := decodePacketValue(buf[n:], values.Index(i))
				n += n2
				if err != nil {
					return n, err
				}
			}
		case reflect.Uint8:
			ln := int(int32(binary.BigEndian.Uint32(buf[n : n+4])))
			if ln < 0 {
				n += 4
				v.SetBytes(nil)
			} else {
				bytes := make([]byte, ln)
				copy(bytes, buf[n+4:n+4+ln])
				v.SetBytes(bytes)
				n += 4 + ln
			}
		}
	}
	return n, nil
}

func EncodePacket(buf []byte, st interface{}) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(runtime.Error); ok && e.Error() == "runtime error: slice bounds out of range" {
				err = errShortBuffer
			} else {
				panic(r)
			}
		}
	}()

	v := reflect.ValueOf(st)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0, errPtrExpected
	}
	return encodePacketValue(buf, v)
}

func encodePacketValue(buf []byte, v reflect.Value) (int, error) {
	rv := v
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	n := 0
	switch v.Kind() {
	default:
		return n, errUnhandledFieldType
	case reflect.Struct:
		if en, ok := rv.Interface().(encoder); ok {
			return en.Encode(buf)
		} else if en, ok := v.Interface().(encoder); ok {
			return en.Encode(buf)
		} else {
			for i := 0; i < v.NumField(); i++ {
				field := v.Field(i)
				n2, err := encodePacketValue(buf[n:], field)
				n += n2
				if err != nil {
					return n, err
				}
			}
		}
	case reflect.Bool:
		if v.Bool() {
			buf[n] = 1
		} else {
			buf[n] = 0
		}
		n++
	case reflect.Int32:
		binary.BigEndian.PutUint32(buf[n:n+4], uint32(v.Int()))
		n += 4
	case reflect.Int64:
		binary.BigEndian.PutUint64(buf[n:n+8], uint64(v.Int()))
		n += 8
	case reflect.String:
		str := v.String()
		binary.BigEndian.PutUint32(buf[n:n+4], uint32(len(str)))
		copy(buf[n+4:n+4+len(str)], []byte(str))
		n += 4 + len(str)
	case reflect.Slice:
		switch v.Type().Elem().Kind() {
		default:
			count := v.Len()
			startN := n
			n += 4
			for i := 0; i < count; i++ {
				n2, err := encodePacketValue(buf[n:], v.Index(i))
				n += n2
				if err != nil {
					return n, err
				}
			}
			binary.BigEndian.PutUint32(buf[startN:startN+4], uint32(count))
		case reflect.Uint8:
			if v.IsNil() {
				binary.BigEndian.PutUint32(buf[n:n+4], uint32(0xffffffff))
				n += 4
			} else {
				bytes := v.Bytes()
				binary.BigEndian.PutUint32(buf[n:n+4], uint32(len(bytes)))
				copy(buf[n+4:n+4+len(bytes)], bytes)
				n += 4 + len(bytes)
			}
		}
	}
	return n, nil
}

func RequestStructForOp(op int32) interface{} {
	switch op {
	case OpClose:
		return &CloseRequest{}
	case OpCreate:
		return &CreateRequest{}
	case OpDelete:
		return &DeleteRequest{}
	case OpExists:
		return &ExistsRequest{}
	case OpGetAcl:
		return &GetAclRequest{}
	case OpGetChildren:
		return &GetChildrenRequest{}
	case OpGetChildren2:
		return &GetChildren2Request{}
	case OpGetData:
		return &GetDataRequest{}
	case OpPing:
		return &PingRequest{}
	case OpSetAcl:
		return &SetAclRequest{}
	case OpSetData:
		return &SetDataRequest{}
	case OpSetWatches:
		return &SetWatchesRequest{}
	case OpSync:
		return &SyncRequest{}
	case OpSetAuth:
		return &SetAuthRequest{}
	case OpCheck:
		return &CheckVersionRequest{}
	case OpMulti:
		return &MultiRequest{}
	}
	return nil
}
