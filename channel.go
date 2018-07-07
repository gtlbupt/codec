package codec

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type ChannelOptions struct {
	ConnectionTimeoutMs time.Duration
	TimeoutMs           time.Duration
	MaxRetry            int
}

type Channel struct {
	sync.Mutex
	Options   ChannelOptions
	clients   []*rpc.Client
	lastIndex int
}

func (c *Channel) Init(nsUrl string, lb string, options ChannelOptions) error {
	c.Options = options
	c.clients = make([]*rpc.Client, 0, 16)
	c.lastIndex = 0

	if !strings.HasPrefix(nsUrl, "list://") {
		return fmt.Errorf("Bad NsUrl:%s", nsUrl)
	}

	var ipPorts = strings.TrimPrefix(nsUrl, "list://")
	for _, ipPort := range strings.Split(ipPorts, ",") {
		var conn, err = net.Dial("tcp", ipPort)
		if err != nil {
			return err
		}
		var client = rpc.NewClientWithCodec(NewClientCodec(conn))
		c.clients = append(c.clients, client)
	}
	return nil
}

func (c *Channel) CallMethod(ServiceMethod string, request proto.Message, response proto.Message) error {
	var idx = 0
	c.Lock()
	if idx >= len(c.clients) {
		return fmt.Errorf("Bad Server Index")
	}
	var client = c.clients[idx]
	c.Unlock()
	return client.Call(ServiceMethod, request, response)
}
