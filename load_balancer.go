package codec

import (
	"net/rpc"
)

const (
	E_OK        = 0
	E_NO_DATA   = 1
	E_HOST_DOWN = 2
)

type ServerId string

type LoadBalancerSelectIn struct {
	lb string
}

type LoadBalancerSelectOut struct {
	client *rpc.Client
}

type LoadBalancer interface {
	AddServer(serverId ServerId) bool
	RemoveServer(serverId ServerId) bool
	AddServersInBatch(serverIds []ServerId) int
	RemoveServersInBatch(serverIds []ServerId) int
	SelectServer(in LoadBalancerSelectIn, out *LoadBalancerSelectOut) int
}

type LoadBalancerServers struct {
	ServerList []ServerId
	ServerMap  map[ServerId]int
	LastIndex  int
}

type RoundRobinLoadBalancer struct {
	servers LoadBalancerServers
}

func (lb *RoundRobinLoadBalancer) AddServer(serverId ServerId) bool {
	if _, ok := lb.servers.ServerMap[serverId]; ok {
		return false
	}
	var idx = len(lb.servers.ServerList)
	lb.servers.ServerMap[serverId] = idx
	lb.servers.ServerList = append(lb.servers.ServerList, serverId)
	return true
}

func (lb *RoundRobinLoadBalancer) RemoveServer(serverId ServerId) bool {
	idx, exist := lb.servers.ServerMap[serverId]
	if !exist {
		return false
	}

	var list = lb.servers.ServerList
	lb.servers.ServerList = append(list[0:idx], list[idx:]...)
	delete(lb.servers.ServerMap, serverId)
	return true
}

func (lb *RoundRobinLoadBalancer) AddServersInBatch(serverIds []ServerId) int {
	var count = 0
	for _, serverId := range serverIds {
		var ok = lb.AddServer(serverId)
		if ok {
			count += 1
		}
	}
	return count
}

func (lb *RoundRobinLoadBalancer) RemoveServersInBatch(serverIds []ServerId) int {
	var count = 0
	for _, serverId := range serverIds {
		var ok = lb.RemoveServer(serverId)
		if ok {
			count += 1
		}
	}
	return count
}

func (lb *RoundRobinLoadBalancer) SelectServer(in LoadBalancerSelectIn, out LoadBalancerSelectOut) int {
	var n = len(lb.servers.ServerList)
	if n == 0 {
		return ENODATA
	}

	var idx = lb.LastIndex % n
	lb.LastIndex++

	var serverId = l.servers.ServerMap[idx]
	var client *rpc.Client = nil
	for i := 0; i < n; i++ {
		client = GlobalRpcClientAddr(serverId)
		if client != nil {
			out.client = client
			return
		}
	}

	return E_HOST_DOWN
}
