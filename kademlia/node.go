package kademlia

import (
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type NodeInf struct {
	Addr     string
	Identify big.Int
}

type Node struct {
	online     bool
	onlineLock sync.RWMutex
	quitChan   chan bool

	listener net.Listener
	server   *rpc.Server

	Addr     string
	Identify big.Int

	data    DataType
	KBucket [kHashSize]Bucket
}

func init() {
	f, _ := os.Create("kademlia-test.log")
	logrus.SetOutput(f)
}

// RemoteCall calls the RPC method at addr.
func RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if addr == "" {
		logrus.Errorf("[RemoteCall] [%s] with a empty address", method)
		return fmt.Errorf("[RemoteCall] with a empty address")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		if method != "Node.PingRPC" {
			logrus.Errorf("[RemoteCall] [%s] error:%s", method, err)
		}
		return err
	}

	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Errorf("[RemoteCall] client calling %s %s %s error:%s", method, args, reply, err)
	}
	return nil
}

//
// RPC Methods
//

// PING RPC Probes a node to see if it's online
func (node *Node) PingRPC(_ string, _ *string) error {
	return nil
}

type SPair struct{
	Sender string
	Pair Pair
}

func (node *Node) Putin(sp SPair, _ *string) error {
	pair:=sp.Pair
	node.data.put(pair.Key, pair.Value)
	node.update(sp.Sender)
	return nil
}

func (node *Node) PutinData(rt RepublishType, _ *string) error {
	for key, value := range rt.Remove {
		node.data.put(key,value)
	}
	addr := rt.Sender
	if addr == "" || addr == node.Addr {
		return nil
	}
	ind := cpl(Hash(addr), Hash(node.Addr))
	node.KBucket[ind].delete(addr)
	return nil
}

// Store instructs a node to store a <key,value> pair for later retrieval
func (node *Node) Store(pair Pair, _ *string) error {
	retList:=node.Lookup(pair.Key)
	var empty string
	for i := 0; i < retList.Size; i++ {
		RemoteCall(retList.PeerAddr[i], "Node.Putin", SPair{node.Addr,pair}, &empty)
	}
	return nil
}

type STtype struct {
	Sender string
	Target string
}

// return NodeInf for the k nodes it knows about closest to the addr
func (node *Node) FindNode(addr string, retList *RetList) error {
	retList.Init(addr)
	ind := cpl(Hash(addr), Hash(node.Addr))
	if ind < 0 {
		retList.Insert(addr)
	} else {
		node.KBucket[ind].mu.RLock()
		for i := 0; i < node.KBucket[ind].list.Size; i++ {
			retList.Insert(node.KBucket[ind].list.PeerAddr[i])
		}
		node.KBucket[ind].mu.RUnlock()
	}
	if retList.Size == k {
		return nil
	}
	for i := ind - 1; i >= 0; i-- {
		node.KBucket[i].mu.RLock()
		for j := 0; j < node.KBucket[i].list.Size; j++ {
			retList.Insert(node.KBucket[i].list.PeerAddr[j])
		}
		node.KBucket[i].mu.RUnlock()
	}
	retList.Insert(node.Addr)
	if retList.Size == k {
		return nil
	}
	for i := ind + 1; i < kHashSize; i++ {
		node.KBucket[i].mu.RLock()
		for j := 0; j < node.KBucket[i].list.Size; j++ {
			retList.Insert(node.KBucket[i].list.PeerAddr[j])
		}
		node.KBucket[i].mu.RUnlock()
	}
	return nil
}

func (node *Node) RPCFindNode(st STtype, retList *RetList) error {
	err := node.FindNode(st.Target, retList)
	node.update(st.Sender)
	return err
}

type FindValueReply struct {
	Value string
	List  RetList
}

func (node *Node) FindValue(key string, ret *FindValueReply) error {
	ok, value := node.data.get(key)
	if ok {
		*ret = FindValueReply{value, RetList{}}
		return nil
	}
	var retList RetList
	node.FindNode(key, &retList)
	*ret = FindValueReply{"", retList}
	return nil
}

func (node *Node) RPCFindValue(st STtype, ret *FindValueReply) error {
	err := node.FindValue(st.Target, ret)
	node.update(st.Sender)
	return err
}

//
// DHT methods for interfaces
//

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	node.quitChan = make(chan bool, 1)

	node.Addr = addr
	node.Identify = *Hash(addr)

	node.data.Init()
	for i := 0; i < kHashSize; i++ {
		node.KBucket[i].Init(node.Addr)
	}
}

// "Run" is called after calling "NewNode". Some initialization works are done.
func (node *Node) Run() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		logrus.Errorf("[RunRPCServer] [%s] Register error:%s", node.Addr, err)
		return
	}

	node.listener, err = net.Listen("tcp", node.Addr)
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}

	node.onlineLock.Lock()
	node.online = true
	node.onlineLock.Unlock()

	logrus.Infof("[Run] %s", node.Addr)
	go func() {
		for node.online {
			select {
			case <-node.quitChan:
				logrus.Infof("[StopRPCServer] [%s] stopped RPC server", node.Addr)
				return
			default:
				conn, err := node.listener.Accept()
				if err != nil {
					logrus.Errorf("[RunRPCServer] [%s] accept error:%s", node.Addr, err)
					return
				}
				go node.server.ServeConn(conn)
			}
		}
		logrus.Infof("[StopRPCServer] [%s] stopped RPC server", node.Addr)
	}()
}

// "Create" or "Join" will be called after calling "Run".
// For a dhtNode, either "Create" or "Join" will be called, but not both.

// Create a new network.
func (node *Node) Create() {
	logrus.Infof("[Create] [%s]", node.Addr)
	node.maintain()
}

// Join an existing network. Return "true" if join succeeded and "false" if not.
func (node *Node) Join(addr string) bool {
	logrus.Infof("[Join] join [%s] to %s", node.Addr, addr)
	node.update(addr)
	node.Lookup(node.Addr)
	logrus.Infof("[Join] [%s] joined successfully", node.Addr)
	node.maintain()
	return true
}

// "Normally" quit from current network, can inform other nodes in the network that you are leaving.
// "Quit" will not be called before "Create" or "Join".
// For a dhtNode, "Quit" may be called for many times.
// For a quited node, call "Quit" again should have no effect.

func (node *Node) Quit() {
	if !node.online {
		logrus.Warnf("[Quit] [%s] is offline", node.Addr)
		return
	}

	logrus.Infof("Quit %s", node.Addr)

	node.republishAll()

	node.quitChan <- true
	err := node.listener.Close()

	if err != nil {
		logrus.Errorf("[Quit] [%s] error: %s", node.Addr, err)
	}

	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	node.quitChan = make(chan bool, 1)

	node.data.clear()

	logrus.Infof("[Quit] [%s] success", node.Addr)
}

// Quit the network without informing other nodes.
func (node *Node) ForceQuit() {
	if !node.online {
		logrus.Warnf("[ForceQuit] [%s] is offline", node.Addr)
		return
	}
	logrus.Infof("[ForceQuit] %s", node.Addr)
	node.quitChan <- true
	err := node.listener.Close()

	if err != nil {
		logrus.Errorf("[ForceQuit] [%s] error: %s", node.Addr, err)
	}

	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	node.quitChan = make(chan bool, 1)
	node.data.clear()
	logrus.Infof("[ForceQuit] [%s] success", node.Addr)
}

// Check whether the node identified by addr is in the network.
func (node *Node) Ping(addr string) bool {
	var empty string
	err := RemoteCall(addr, "Node.PingRPC", "", &empty)
	if err == nil {
		return true
	} else {
		return false
	}
}

// Put a key-value pair into the network (if key exists, Update the value).
// Return "true" if success, "false" otherwise.
func (node *Node) Put(key string, value string) bool {
	var empty string
	err := node.Store(Pair{key, value}, &empty)
	// logrus.Infof("[Put] [%s] %s", node.Addr, key)
	return err == nil
}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
func (node *Node) Get(key string) (bool, string) {
	var ret FindValueReply
	err := node.FindValue(key, &ret)
	if err != nil {
		logrus.Errorf("[Get] [%s] fail to get %s", node.Addr, key)
		return false, ""
	}
	if ret.Value != "" {
		return true, ret.Value
	}
	// logrus.Infof("[Get] [%s] %s", node.Addr, key)
	return node.getvalue(key, ret.List)
}

// Remove a key-value pair identified by KEY from the network.
// Return "true" if success, "false" otherwise.
func (node *Node) Delete(key string) bool { // kademlia协议不支持删除
	return true
}

//
// local func
//

// 找到所有节点中离addr最近k个节点
func (node *Node) Lookup(addr string) RetList {
	var retList RetList
	err := node.FindNode(addr, &retList)
	if err != nil {
		logrus.Errorf("[lookup] %s fail", addr)
	}
	updated := true
	visited := make(map[string]bool)
	for updated {
		updated = false
		var invalid []string
		var tmpList RetList
		tmpList.Init(addr)
		for i := 0; i < retList.Size; i++ {
			if visited[retList.PeerAddr[i]] {
				continue
			}
			visited[retList.PeerAddr[i]] = true
			node.update(retList.PeerAddr[i])

			var ret RetList
			err = RemoteCall(retList.PeerAddr[i], "Node.RPCFindNode", STtype{node.Addr, addr}, &ret)
			if err != nil {
				logrus.Warnf("[FindNode] [%s] fail to find %s", retList.PeerAddr[i], addr)
				invalid = append(invalid, retList.PeerAddr[i])
			} else {
				for j := 0; j < ret.Size; j++ {
					tmpList.Insert(ret.PeerAddr[j])
				}
			}
		}
		for _, v := range invalid {
			retList.Delete(v)
			updated = true
		}
		for i := 0; i < tmpList.Size; i++ {
			if retList.Insert(tmpList.PeerAddr[i]) {
				updated = true
			}
		}
	}
	return retList
}

func (node *Node) getvalue(key string, retList RetList) (bool, string) {
	updated := true
	visited := make(map[string]bool)
	// defer logrus.Infof("[getValue] [%s] %s", node.Addr, key)
	for updated {
		updated = false
		var invalid []string
		var tmpList RetList
		tmpList.Init(key)
		for i := 0; i < retList.Size; i++ {
			if visited[retList.PeerAddr[i]] {
				continue
			}
			visited[retList.PeerAddr[i]] = true
			node.update(retList.PeerAddr[i])

			var ret FindValueReply
			ret.List.Init(key)
			err := RemoteCall(retList.PeerAddr[i], "Node.RPCFindValue", STtype{node.Addr, key}, &ret)
			if err != nil {
				logrus.Warnf("[FindValue] [%s] fail to find %s", retList.PeerAddr[i], key)
				invalid = append(invalid, retList.PeerAddr[i])
			} else {
				if ret.Value != "" {
					return true, ret.Value
				}
				for j := 0; j < ret.List.Size; j++ {
					tmpList.Insert(ret.List.PeerAddr[j])
				}
			}
		}
		for _, v := range invalid {
			retList.Delete(v)
			updated = true
		}
		for i := 0; i < tmpList.Size; i++ {
			if retList.Insert(tmpList.PeerAddr[i]) {
				updated = true
			}
		}
	}
	return false, ""
}

func (node *Node) update(addr string) {
	if addr == "" || addr == node.Addr || !node.Ping(addr) {
		return
	}
	ind := cpl(Hash(addr), Hash(node.Addr))
	node.KBucket[ind].update(addr)
}

func (node *Node) republish() {
	var empty string
	republishList := node.data.republish()
	for key, value := range republishList {
		node.Store(Pair{key, value}, &empty)
	}
}

type RepublishType struct{
	Sender string
	Remove map[string]string
}

func (node *Node) republishAll() {
	var rt RepublishType
	rt.Sender=node.Addr
	rt.Remove = make(map[string]string)
	node.data.mu.RLock()
	for k, v := range node.data.Data {
		rt.Remove[k] = v
	}
	node.data.mu.RUnlock()
	var nameList RetList
	node.FindNode(node.Addr, &nameList)
	nameList.Delete(node.Addr)
	var empty string
	var wg sync.WaitGroup
	wg.Add(k)
	for _, n := range nameList.PeerAddr {
		if n == "" {
			wg.Done()
			continue
		}
		var name = n
		go func() {
			RemoteCall(name, "Node.PutinData", rt, &empty)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (node *Node) expire() {
	node.data.expire()
}

func (node *Node) maintain() {
	go func() {
		for node.online {
			node.republish()
			time.Sleep(RepublishInterval)
		}
	}()
	go func() {
		for node.online {
			node.expire()
			time.Sleep(ExpireInterval)
		}
	}()
}
