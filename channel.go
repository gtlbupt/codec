package codec

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"math/rand"
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

const (
	LOAD_BALANCER_ROUND_RoBIN = 1
	LOAD_BALANCER_RANDOM      = 2
)

type Channel struct {
	sync.Mutex
	Options   ChannelOptions
	clients   []*rpc.Client
	lastIndex int
	lb        string
	r         *rand.Rand
}

func (c *Channel) Init(nsUrl string, lb string, options ChannelOptions) error {
	c.Options = options
	c.clients = make([]*rpc.Client, 0, 16)
	c.lastIndex = 0
	c.lb = lb
	c.r = rand.New(rand.NewSource(time.Now().UnixNano()))

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
	c.Lock()
	var idx = c.getIndex()
	if idx == -1 {
		c.Unlock()
		return fmt.Errorf("Bad Server Index")
	}

	var client = c.clients[idx]
	c.Unlock()
	return client.Call(ServiceMethod, request, response)
}

func (c *Channel) getIndex() int {
	var idx = 0
	var lb = c.lb
	if lb == "rr" {
		idx = c.lastIndex
		c.lastIndex++
	} else if lb == "random" {
		idx = c.r.Int()
	} else if lb == "c_md5" {
	} else {
	}

	if len(c.clients) > 0 {
		return idx % len(c.clients)
	} else {
		return -1
	}
}
