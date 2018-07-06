package codec

import (
	"bufio"
	"fmt"
	"io"
	"net/rpc"
	"sync"

	brpc "./proto"
	"github.com/golang/protobuf/proto"
)

const defaultBufferSize = 4 * 1024

type serverCodec struct {
	mu   sync.Mutex // exclusive writer lock
	resp brpc.RpcMeta
	enc  *Encoder
	w    *bufio.Writer

	req brpc.RpcMeta
	dec *Decoder
	c   io.Closer
}

// NewServerCodec returns a new rpc.ServerCodec.
//
// A ServerCodec implements reading of RPC requests and writing of RPC
// responses for the server side of an RPC session. The server calls
// ReadRequestHeader and ReadRequestBody in pairs to read requests from the
// connection, and it calls WriteResponse to write a response back. The
// server calls Close when finished with the connection. ReadRequestBody
// may be called with a nil argument to force the body of the request to be
// read and discarded.
func NewServerCodec(rwc io.ReadWriteCloser) rpc.ServerCodec {
	w := bufio.NewWriterSize(rwc, defaultBufferSize)
	r := bufio.NewReaderSize(rwc, defaultBufferSize)
	return &serverCodec{
		enc: NewEncoder(w),
		w:   w,
		dec: NewDecoder(r),
		c:   rwc,
	}
}

func (c *serverCodec) WriteResponse(resp *rpc.Response, body interface{}) error {
	c.mu.Lock()
	c.resp.Response = &brpc.RpcResponseMeta{
		ErrorCode: proto.Int32(0),
		ErrorText: proto.String(""),
	}
	c.resp.CorrelationId = proto.Int64(int64(resp.Seq))

	{
		var buf, err = proto.Marshal(&c.resp)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		c.w.Write(buf)
	}

	{
		pb, ok := body.(proto.Message)
		if !ok {
			return fmt.Errorf("body not proto.Message")
		}
		var buf, err = proto.Marshal(pb)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		c.w.Write(buf)
	}

	var err = c.w.Flush()
	c.mu.Unlock()
	return err
}

func (c *serverCodec) ReadRequestHeader(req *rpc.Request) error {
	c.req.Reset()
	if err := c.dec.Decode(&c.req); err != nil {
		return err
	}

	req.ServiceMethod = fmt.Sprintf("%s.%s", c.req.Request.ServiceName, c.req.Request.MethodName)
	req.Seq = uint64(*c.req.CorrelationId)
	return nil
}

func (c *serverCodec) ReadRequestBody(body interface{}) error {
	if pb, ok := body.(proto.Message); ok {
		return c.dec.DecodeRequestBody(pb)
	}
	return fmt.Errorf("%T does not implement proto.Message", body)
}

func (c *serverCodec) Close() error { return c.c.Close() }
