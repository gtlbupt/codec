package codec

import (
	"bytes"
	"fmt"
	"net"
	"net/rpc"
	"reflect"
	"testing"

	example "./example"
	"./internal"

	"github.com/golang/protobuf/proto"
)

type buffer struct {
	*bytes.Buffer
}

func (c buffer) Close() error { return nil }

func TestReq(t *testing.T) {
	var conn, err = net.Dial("tcp", "127.0.0.1:8170")
	if err != nil {
		t.Errorf("[err:%v]", err)
	}

	var client = rpc.NewClientWithCodec(NewClientCodec(conn))

	var req = example.EchoRequest{
		Message: proto.String("Hello World"),
	}
	var resp = example.EchoResponse{}
	resp.Reset()
	err = client.Call("EchoService.Echo", &req, &resp)
	fmt.Printf("[err:%v][req:%v][resp:%v]", err, req, resp)
}

func TestClientCodecBasic(t *testing.T) {
	t.Parallel()

	buf := buffer{Buffer: &bytes.Buffer{}}
	cc := NewClientCodec(buf)
	sc := NewServerCodec(buf)

	req := rpc.Request{
		ServiceMethod: "test.service.method",
		Seq:           1<<64 - 1,
	}
	resp := rpc.Request{}
	body := internal.Struct{}

	if err := cc.WriteRequest(&req, &testMessage); err != nil {
		t.Fatalf("write client request: %v", err)
	}

	if err := sc.ReadRequestHeader(&resp); err != nil {
		t.Fatalf("read client request header: %v", err)
	}
	if err := sc.ReadRequestBody(&body); err != nil {
		t.Fatalf("read client request body: %v", err)
	}

	if !reflect.DeepEqual(req, resp) {
		t.Fatalf("encode/decode requeset header: expected %#v, got %#v", req, resp)
	}
	if !reflect.DeepEqual(testMessage, body) {
		t.Fatalf("encode/decode request body: expected %#v, got %#v", testMessage, body)
	}
}

func TestServerCodecBasic(t *testing.T) {
	t.Parallel()

	buf := buffer{Buffer: &bytes.Buffer{}}
	cc := NewClientCodec(buf)
	sc := NewServerCodec(buf)

	req := rpc.Response{
		ServiceMethod: "test.service.method",
		Seq:           1<<64 - 1,
		Error:         "test error message",
	}
	resp := rpc.Response{}
	body := internal.Struct{}

	if err := sc.WriteResponse(&req, &testMessage); err != nil {
		t.Fatalf("write server response: %v", err)
	}

	if err := cc.ReadResponseHeader(&resp); err != nil {
		t.Fatalf("read server response header: %v", err)
	}
	if err := cc.ReadResponseBody(&body); err != nil {
		t.Fatalf("read server request body: %v", err)
	}

	if !reflect.DeepEqual(req, resp) {
		t.Fatalf("encode/decode response header: expected %#v, got %#v", req, resp)
	}
	if !reflect.DeepEqual(testMessage, body) {
		t.Fatalf("encode/decode response body: expected %#v, got %#v", testMessage, body)
	}
}
