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

	data    Data
	KBucket [kHashSize]Bucket
}

func init() {
	f, _ := os.Create("kademlia-test.log")
	logrus.SetOutput(f)
}

// RemoteCall calls the RPC method at addr.
func RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if addr == "" {
		logrus.Error("[RemoteCall] with a empty address")
		return fmt.Errorf("[RemoteCall] with a empty address")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		logrus.Errorf("[RemoteCall] dialing %s error:%s", addr, err)
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

func (node *Node) Putin(pair Pair, sender *string) error {
	node.data.put(pair.key, pair.value)
	node.update(*sender)
	return nil
}

// Store instructs a node to store a <key,value> pair for later retrieval
func (node *Node) Store(pair Pair, _ *string) error {
	retList := node.lookup(pair.key)
	p := retList.Rlist.Head.Next
	for p != retList.Rlist.Rear {
		err := RemoteCall(p.Addr, "Node.Putin", pair, &node.Addr)
		if err != nil {
			logrus.Errorf("[Store] [%s] fail to put", p.Addr)
			return fmt.Errorf("[Store] [%s] fail to put", p.Addr)
		}
		p = p.Next
	}
	return nil
}

// return NodeInf for the k nodes it knows about closest to the target ID
func (node *Node) FindNode(addr string, retList *RetList) error {
	retList.Rlist.Init(addr)
	ind := cpl(Hash(addr), Hash(node.Addr))
	if ind < 0 {
		retList.insert(addr)
	} else {
		node.KBucket[ind].mu.RLock()
		p := node.KBucket[ind].list.Head.Next
		for p != node.KBucket[ind].list.Rear {
			retList.insert(p.Addr)
			p = p.Next
		}
		node.KBucket[ind].mu.RUnlock()
	}
	logrus.Warn("1111")
	if retList.Rlist.Size == k {
		return nil
	}
	for i := ind - 1; i >= 0; i-- {
		node.KBucket[i].mu.RLock()
		p := node.KBucket[i].list.Head.Next
		for p != node.KBucket[i].list.Rear {
			retList.insert(p.Addr)
			p = p.Next
		}
		node.KBucket[i].mu.RUnlock()
	}
	if retList.Rlist.Size == k {
		return nil
	}
	for i := ind + 1; i < kHashSize; i++ {
		node.KBucket[i].mu.RLock()
		p := node.KBucket[i].list.Head.Next
		for p != node.KBucket[i].list.Rear {
			retList.insert(p.Addr)
			p = p.Next
		}
		node.KBucket[i].mu.RUnlock()
	}
	return nil
}

type findValueReply struct {
	value   string
	retList RetList
}

func (node *Node) FindValue(key string, ret *findValueReply) error {
	ok, value := node.data.get(key)
	if ok {
		*ret = findValueReply{value, RetList{}}
		return nil
	}
	var retList RetList
	node.FindNode(key, &retList)
	// logrus.Warnf("<[%s]>%v",retList.Rlist.nodeAddr,retList.Rlist.Size)
	*ret = findValueReply{"", retList}
	return nil
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
	node.lookup(node.Addr)
	logrus.Infof("[Join] [%s] joined successfully", node.Addr)
	node.maintain()
	return true
}

// "Normally" quit from current network.
// You can inform other nodes in the network that you are leaving.
// "Quit" will not be called before "Create" or "Join".
// For a dhtNode, "Quit" may be called for many times.
// For a quited node, call "Quit" again should have no effect.

func (node *Node) Quit() {
	if !node.online {
		logrus.Warnf("[Quit] [%s] is offline", node.Addr)
		return
	}

	logrus.Infof("Quit %s", node.Addr)
	node.quitChan <- true
	err := node.listener.Close()

	if err != nil {
		logrus.Errorf("[Quit] [%s] error: %s", node.Addr, err)
	}

	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	node.quitChan = make(chan bool, 1)

	node.republishAll()
	node.data.clear()

	logrus.Infof("[Quit] [%s] success", node.Addr)
}

// Quit the network without informing other nodes.
// "ForceQuit" will be checked by TA manually.
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

// Put a key-value pair into the network (if key exists, update the value).
// Return "true" if success, "false" otherwise.
func (node *Node) Put(key string, value string) bool {
	if !node.online {
		logrus.Errorf("[Put] [%s] is offline", node.Addr)
		return false
	}
	err := node.Store(Pair{key, value}, &node.Addr)
	return err == nil
}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
func (node *Node) Get(key string) (bool, string) {
	if !node.online {
		logrus.Errorf("[Get] [%s] is offline", node.Addr)
		return false, ""
	}
	var ret findValueReply
	err := node.FindValue(key, &ret)
	if err != nil {
		logrus.Errorf("[Get] [%s] fail to get %s", node.Addr, key)
		return false, ""
	}
	if ret.value != "" {
		return true, ret.value
	}
	return node.getvalue(key, ret.retList)
}

// Remove a key-value pair identified by KEY from the network.
// Return "true" if success, "false" otherwise.
func (node *Node) Delete(key string) bool {
	if !node.online {
		logrus.Errorf("[Delete] [%s] is offline", node.Addr)
		return false
	}
	return true
}

//
// local func
//

// kBucket中离addr最近的k个节点
func (node *Node) findNode(addr string) RetList {
	var retList RetList
	retList.Rlist.Init(addr)
	ind := cpl(Hash(addr), Hash(node.Addr))
	if ind < 0 {
		retList.insert(addr)
	} else {
		node.KBucket[ind].mu.RLock()
		p := node.KBucket[ind].list.Head.Next
		for p != node.KBucket[ind].list.Rear {
			retList.insert(p.Addr)
			p = p.Next
		}
		node.KBucket[ind].mu.RUnlock()
	}
	if retList.Rlist.Size == k {
		return retList
	}
	for i := ind - 1; i >= 0; i-- {
		node.KBucket[i].mu.RLock()
		p := node.KBucket[i].list.Head.Next
		for p != node.KBucket[i].list.Rear {
			retList.insert(p.Addr)
			p = p.Next
		}
		node.KBucket[i].mu.RUnlock()
	}
	if retList.Rlist.Size == k {
		return retList
	}
	for i := ind + 1; i < kHashSize; i++ {
		node.KBucket[i].mu.RLock()
		p := node.KBucket[i].list.Head.Next
		for p != node.KBucket[i].list.Rear {
			retList.insert(p.Addr)
			p = p.Next
		}
		node.KBucket[i].mu.RUnlock()
	}
	return retList
}

// 找到所有节点中离addr最近k个节点
func (node *Node) lookup(addr string) RetList {
	retList := node.findNode(addr)

	updated := true
	visited := make(map[string]bool)
	for updated {
		updated = false
		p := retList.Rlist.Head.Next
		var invalid []string
		var tmpList RetList
		tmpList.Rlist.Init(addr)
		for p != retList.Rlist.Rear {
			if visited[p.Addr] {
				continue
			}
			visited[p.Addr] = true
			node.update(p.Addr)

			var ret RetList
			ret.Rlist.Init(addr)
			err := RemoteCall(p.Addr, "Node.FindNode", addr, &ret)
			if err != nil {
				logrus.Errorf("[FindNode] [%s] fail to find %s", p.Addr, addr)
				invalid = append(invalid, p.Addr)
			} else {
				pp := ret.Rlist.Head.Next
				for pp != ret.Rlist.Rear {
					tmpList.insert(pp.Addr)
					pp = pp.Next
				}
			}
			p = p.Next
		}
		for _, v := range invalid {
			retList.delete(v)
		}
		p = tmpList.Rlist.Head.Next
		for p != tmpList.Rlist.Rear {
			if retList.insert(p.Addr) {
				updated = true
			}
		}
	}
	return retList
}

func (node *Node) getvalue(key string, retList RetList) (bool, string) {
	// logrus.Warnf("<[%s]>%v",retList.Rlist.nodeAddr,retList.Rlist.Size)
	updated := true
	visited := make(map[string]bool)
	for updated {
		// logrus.Warn("1")
		updated = false
		p := retList.Rlist.Head.Next
		var invalid []string
		var tmpList RetList
		tmpList.Rlist.Init(key)
		for p != retList.Rlist.Rear {
			logrus.Warn("2")
			if visited[p.Addr] {
				continue
			}
			visited[p.Addr] = true
			node.update(p.Addr)

			var ret findValueReply
			ret.retList.Rlist.Init(key)
			err := RemoteCall(p.Addr, "Node.FindValue", key, &ret)
			if err != nil {
				logrus.Errorf("[FindValue [%s] fail to find %s", p.Addr, key)
				invalid = append(invalid, p.Addr)
			} else {
				logrus.Warn("4")
				if ret.value != "" {
					return true, ret.value
				}
				pp := ret.retList.Rlist.Head.Next
				for pp != ret.retList.Rlist.Rear {
					tmpList.insert(pp.Addr)
					pp = pp.Next
				}
			}
			p = p.Next
		}
		for _, v := range invalid {
			retList.delete(v)
		}
		p = tmpList.Rlist.Head.Next
		for p != tmpList.Rlist.Rear {
			if retList.insert(p.Addr) {
				updated = true
			}
		}
	}
	// logrus.Warn("3")
	return false, ""
}

func (node *Node) update(addr string) {
	if addr == "" || addr == node.Addr {
		return
	}
	ind := cpl(Hash(addr), Hash(node.Addr))
	if ind < 0 {
		logrus.Error("[update] error in index")
	}
	node.KBucket[ind].update(addr)

}

func (node *Node) republish() {
	republishList := node.data.republish()
	for _, key := range republishList {
		node.data.mu.RLock()
		node.Put(key, node.data.data[key])
		node.data.mu.RUnlock()
	}
}

func (node *Node) republishAll() {
	node.data.mu.RLock()
	for key := range node.data.data {
		node.Put(key, node.data.data[key])
	}
	node.data.mu.RUnlock()
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
