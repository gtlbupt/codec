package codec

import (
	"net/rpc"
	"sync"
)

type NamingServiceWatcher interface {
	OnAddServers(serverIds []*rpc.Client) bool
	OnRemoveServers(serverIds []*rpc.Client) bool
}

type NamingServiceThread struct {
	sync.Mutex
}

func (nst *NamingServiceThread) AddWatcher(watcher *NamingServiceWatcher) {
}

func (nst *NamingServiceThread) RemoveWatcher(watcher *NamingServiceWatcher) {
}

var g_rpc_client_map sync.Map

func GlobalRpcClientAddr(socketId SocketId) *rpc.Client {
	var value, ok = g_rpc_client_map.Load(socketId)
	if ok {
		return value
	} else {
		return nil
	}
}
