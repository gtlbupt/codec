package codec

import (
	"bufio"
	"fmt"
	"io"
	"net/rpc"
	"strings"
	"sync"

	brpc "./proto"
	"github.com/golang/protobuf/proto"
)

type clientCodec struct {
	mu  sync.Mutex // exclusive writer lock
	req brpc.RpcMeta
	enc *Encoder
	w   *bufio.Writer

	resp brpc.RpcMeta
	dec  *Decoder
	c    io.Closer
}

// NewClientCodec returns a new rpc.Client.
//
// A ClientCodec implements writing of RPC requests and reading of RPC
// responses for the client side of an RPC session. The client calls
// WriteRequest to write a request to the connection and calls
// ReadResponseHeader and ReadResponseBody in pairs to read responses. The
// client calls Close when finished with the connection. ReadResponseBody
// may be called with a nil argument to force the body of the response to
// be read and then discarded.
func NewClientCodec(rwc io.ReadWriteCloser) rpc.ClientCodec {
	w := bufio.NewWriterSize(rwc, defaultBufferSize)
	r := bufio.NewReaderSize(rwc, defaultBufferSize)
	return &clientCodec{
		enc: NewEncoder(w),
		w:   w,
		dec: NewDecoder(r),
		c:   rwc,
	}
}

func (c *clientCodec) WriteRequest(req *rpc.Request, body interface{}) error {
	c.mu.Lock()
	{
		var serviceName = ExtractServiceName(req.ServiceMethod)
		var methodName = ExtractMethodName(req.ServiceMethod)
		c.req.Request = &brpc.RpcRequestMeta{
			ServiceName: proto.String(serviceName),
			MethodName:  proto.String(methodName),
		}
		c.req.CorrelationId = proto.Int64(int64(req.Seq))
	}

	var rpcMeta, err = proto.Marshal(&c.req)
	if err != nil {
		c.mu.Unlock()
		return err
	}

	pb, ok := body.(proto.Message)
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("body not proto.Message")
	}

	rpcData, err := proto.Marshal(pb)
	if err != nil {
		c.mu.Unlock()
		return err
	}

	var packetHeader = brpc.PacketHeader{}
	packetHeader.SetMetaSize(len(rpcMeta))
	packetHeader.SetBodySize(len(rpcData))

	{
		var data, _ = packetHeader.Marshal()
		c.w.Write(data)
	}
	{
		c.w.Write(rpcMeta)
	}
	{
		c.w.Write(rpcData)
	}

	err = c.w.Flush()
	c.mu.Unlock()
	return err
}

func (c *clientCodec) ReadResponseHeader(resp *rpc.Response) error {
	c.resp.Reset()
	if err := c.dec.Decode(&c.resp); err != nil {
		return err
	}

	// resp.ServiceMethod = c.resp.Method
	resp.Seq = uint64(*c.resp.CorrelationId)
	// resp.Error = c.resp.Error
	return nil
}

func (c *clientCodec) ReadResponseBody(body interface{}) (err error) {
	if pb, ok := body.(proto.Message); ok {
		return c.dec.Decode(pb)
	}
	return fmt.Errorf("%T does not implement proto.Message", body)
}

func encode(enc *Encoder, m interface{}) (err error) {
	if pb, ok := m.(proto.Message); ok {
		return enc.Encode(pb)
	}
	return fmt.Errorf("%T does not implement proto.Message", m)
}

func (c *clientCodec) Close() error { return c.c.Close() }

func ExtractServiceName(serviceMethod string) string {
	var r = strings.Split(serviceMethod, ".")
	if len(r) > 0 {
		return r[0]
	} else {
		return ""
	}
}

func ExtractMethodName(serviceMethod string) string {
	var r = strings.Split(serviceMethod, ".")
	if len(r) > 1 {
		return r[1]
	} else {
		return ""
	}
}
