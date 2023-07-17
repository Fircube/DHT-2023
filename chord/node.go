package chord

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

func init() {
	f, _ := os.Create("dht-chord-test.log")
	logrus.SetOutput(f)
}

type Pair struct {
	Key   string
	Value string
}

type NodeInf struct {
	Addr     string
	Identify *big.Int
}

type Node struct {
	Addr       string   // addr:port
	Identify   *big.Int // Hash(Address) -> Chord Identifier
	online     bool
	onlineLock sync.RWMutex

	listener net.Listener
	server   *rpc.Server
	quitChan chan bool

	predecessor       NodeInf
	predecessorLock   sync.RWMutex
	successorList     [kSuccessorListSize]NodeInf // several successors to handle node failures
	successorListLock sync.RWMutex
	fingerTable       [kFingerTableSize]NodeInf
	fingerTableLock   sync.RWMutex
	nxtFin            int // stores the index of the next finger to fix. [0,kFingerTableSize-1]

	data       map[string]string
	dataLock   sync.RWMutex
	backup     map[string]string
	backupLock sync.RWMutex
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	node.quitChan = make(chan bool, 1)

	node.Addr = addr
	node.Identify = Hash(addr)

	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()

	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
	// logrus.Infof("[Init] [%s] is inited", addr)
}

// RemoteCall calls the RPC method at addr.
//
// Re-connect to the client every time can be slow. You can use connection pool to improve the performance.
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	// if method != "Node.Ping" && method != "Node.FindSuccessor" {
	// 	logrus.Infof("[RemoteCall] [%s] ask %s to %s with args:%v", node.Addr, addr, method, args)
	// }
	if addr == "" {
		logrus.Error("[RemoteCall] with a empty address")
		return fmt.Errorf("[RemoteCall] with a empty address")
	}

	conn, err := net.Dial("tcp", addr) 
	if err != nil {
		logrus.Errorf("[RemoteCall] [%s] dialing %s error:%s", node.Addr, addr, err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Errorf("[RemoteCall] [%s] client calling %s %s %s error:%s", node.Addr, method, args, reply, err)
	}
	return nil
}

//
// RPC Methods
//

func (node *Node) Ping(objectAddr string, _ *string) error {
	return nil
}

// get
func (node *Node) GetData(_ string, reply *map[string]string) error {
	if !node.online {
		logrus.Errorf("[GetData] [%s] is offline", node.Addr)
		return fmt.Errorf("[GetData] [%s] is offline", node.Addr)
	}
	node.dataLock.RLock()
	*reply = node.data
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) GetPredecessor(_ string, reply *NodeInf) error {
	if !node.online {
		logrus.Errorf("[GetPredecessor] [%s] is offline", node.Addr)
		return fmt.Errorf("[GetPredecessor] [%s] is offline", node.Addr)
	}
	*reply = node.getPredecessor()
	return nil
}

func (node *Node) GetSuccessorList(_ string, reply *[kSuccessorListSize]NodeInf) error {
	if !node.online {
		logrus.Errorf("[GetSuccessorList] [%s] is offline", node.Addr)
		return fmt.Errorf("[GetSuccessorList]] [%s] is offline", node.Addr)
	}
	node.successorListLock.RLock()
	*reply = node.successorList
	node.successorListLock.RUnlock()
	return nil
}

// adjust pre/suc
func (node *Node) FindSuccessor(arg NodeInf, reply *NodeInf) error {
	if !node.online {
		logrus.Errorf("[FindSuccessor] [%s] is offline", node.Addr)
		return fmt.Errorf("[FindSuccessor] [%s] is offline", node.Addr)
	}
	suc := node.getSuccessor()
	if suc.Addr == "" {
		logrus.Errorf("[FindSuccessor] fail to get [%s] 's successor", node.Addr)
		return fmt.Errorf("[FindSuccessor] fail to get [%s] 's successor", node.Addr)
	}
	if (arg.Identify.Cmp(suc.Identify) == 0) || (Contain(arg.Identify, node.Identify, suc.Identify)) {
		*reply = suc
		return nil
	}
	closerNode := node.closestPrecedingFinger(arg.Identify)
	err := node.RemoteCall(closerNode.Addr, "Node.FindSuccessor", arg, reply)
	if err != nil {
		logrus.Errorf("[FindSuccessor] [%s] fail to RemoteCall %s to <FindSuccessor> err: %s", node.Addr, closerNode.Addr, err)
		return fmt.Errorf("[FindSuccessor] [%s] fail to RemoteCall %s to <FindSuccessor>", node.Addr, closerNode.Addr)
	}
	return nil
}

// adjust Data/Backup
func (node *Node) TransferData(objectInf NodeInf, objectData *map[string]string) error {
	if !node.online {
		logrus.Errorf("[TransferData] [%s] is offline", node.Addr)
		return fmt.Errorf("[TransferData] [%s] is offline", node.Addr)
	}
	node.predecessorLock.Lock()
	node.predecessor = objectInf
	node.predecessorLock.Unlock()

	node.dataLock.Lock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	for key, value := range node.data {
		keyId := Hash(key)
		if (keyId.Cmp(node.Identify) != 0) && !(Contain(keyId, objectInf.Identify, node.Identify)) {
			(*objectData)[key] = value
			delete(node.data, key)
			node.backup[key] = value
		}
	}
	node.dataLock.Unlock()
	node.backupLock.Unlock()

	var empty string
	suc := node.getSuccessor()
	err := node.RemoteCall(suc.Addr, "Node.DeleteBackups", *objectData, &empty)
	if err != nil {
		logrus.Errorf("[TransferData] [%s] fail to RemoteCall %s to <DeleteBackups> err: %s", node.Addr, suc.Addr, err)
		return fmt.Errorf("[TransferData] [%s] fail to RemoteCall %s to <DeleteBackups>", node.Addr, suc.Addr)
	}
	return nil
}

func (node *Node) PutBackup(pair Pair, _ *string) error {
	if !node.online {
		logrus.Errorf("[PutBackup] [%s] is offline", node.Addr)
		return fmt.Errorf("[PutBackup] [%s] is offline", node.Addr)
	}
	node.backupLock.Lock()
	node.backup[pair.Key] = pair.Value
	node.backupLock.Unlock()
	return nil
}

func (node *Node) PutBackups(objectData map[string]string, _ *string) error {
	if !node.online {
		logrus.Errorf("[PutBackups] [%s] is offline", node.Addr)
		return fmt.Errorf("[PutBackups] [%s] is offline", node.Addr)
	}
	node.backupLock.Lock()
	for key, value := range objectData {
		node.backup[key] = value
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) DeleteBackup(key string, _ *string) error {
	if !node.online {
		logrus.Errorf("[DeleteBackup] [%s] is offline", node.Addr)
		return fmt.Errorf("[DeleteBackup] [%s] is offline", node.Addr)
	}
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	_, ok := node.backup[key]
	if ok {
		delete(node.backup, key)
	} else {
		logrus.Errorf("[DeleteBackup] [%s] do not have %s in backup", node.Addr, key)
		return fmt.Errorf("[DeleteBackup] [%s] do not have %s in backup", node.Addr, key)
	}
	return nil
}

func (node *Node) DeleteBackups(objectData map[string]string, _ *string) error {
	if !node.online {
		logrus.Errorf("[DeleteBackups] [%s] is offline", node.Addr)
		return fmt.Errorf("[DeleteBackups] [%s] is offline", node.Addr)
	}
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	for key := range objectData {
		_, ok := node.backup[key]
		if ok {
			delete(node.backup, key)
		} else {
			logrus.Errorf("[DeleteBackups] [%s] do not have %s in backup", node.Addr, key)
			return fmt.Errorf("[DeleteBackups] [%s] do not have %s in backup", node.Addr, key)
		}
	}
	return nil
}

// adjust value
func (node *Node) PutValue(pair Pair, _ *string) error {
	if !node.online {
		logrus.Errorf("[PutValue] [%s] is offline", node.Addr)
		return fmt.Errorf("[PutValue] [%s] is offline", node.Addr)
	}
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()

	var empty string
	suc := node.getSuccessor()
	err := node.RemoteCall(suc.Addr, "Node.PutBackup", pair, &empty)
	if err != nil {
		logrus.Errorf("[PutValue] [%s] fail to RemoteCall %s to <PutBackup> err: %s", node.Addr, suc.Addr, err)
		return fmt.Errorf("[PutValue] [%s] fail to RemoteCall %s to <PutBackup>", node.Addr, suc.Addr)
	}
	return nil
}

func (node *Node) GetValue(key string, value *string) error {
	if !node.online {
		logrus.Errorf("[GetValue] [%s] is offline", node.Addr)
		return fmt.Errorf("[GetValue] [%s] is offline", node.Addr)
	}
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()
	var ok bool
	_, ok = node.data[key]
	if ok {
		*value = node.data[key]
		return nil
	} else {
		*value = ""
		logrus.Errorf("[GetValue] [%s] fail to find key:%s", node.Addr, key)
		return fmt.Errorf("[GetValue] [%s] fail to find key:%s", node.Addr, key)
	}
}

func (node *Node) DeleteValue(key string, _ *string) error {
	if !node.online {
		logrus.Errorf("[DeleteValue] [%s] is offline", node.Addr)
		return fmt.Errorf("[DeleteValue] [%s] is offline", node.Addr)
	}
	node.dataLock.RLock()
	_, ok := node.data[key]
	node.dataLock.RUnlock()
	if !ok {
		logrus.Errorf("[DeleteValue] [%s] fail to find key:%s", node.Addr, key)
		return fmt.Errorf("[DeleteValue] [%s] fail to find key:%s", node.Addr, key)
	}
	node.dataLock.Lock()
	delete(node.data, key)
	node.dataLock.Unlock()

	var empty string
	suc := node.getSuccessor()
	err := node.RemoteCall(suc.Addr, "Node.DeleteBackup", key, &empty)
	if err != nil {
		logrus.Errorf("[DeleteValue] [%s] fail to RemoteCall %s to <DeleteBackup> err: %s", node.Addr, suc.Addr, err)
		return fmt.Errorf("[DeleteValue] [%s] fail to RemoteCall %s to <DeleteBackup>", node.Addr, suc.Addr)
	}
	return nil
}

// periodly update
func (node *Node) Stabilize(_ string, _ *string) error {
	if !node.online {
		logrus.Errorf("[Stabilize] [%s] is offline", node.Addr)
		return fmt.Errorf("[Stabilize] [%s] is offline", node.Addr)
	}
	// logrus.Infof("[Stabilize] [%s] start to stabilize",node.Addr)
	suc := node.getSuccessor()
	var sucPre NodeInf
	err := node.RemoteCall(suc.Addr, "Node.GetPredecessor", "", &sucPre)
	if err != nil {
		logrus.Errorf("[Stabilize] [%s] fail to RemoteCall %s to <GetPredecessor> err: %s", node.Addr, suc.Addr, err)
		return fmt.Errorf("[Stabilize] [%s] fail to RemoteCall %s to <GetPredecessor>", node.Addr, suc.Addr)
	}
	if sucPre.Addr != "" && Contain(sucPre.Identify, node.Identify, suc.Identify) {
		suc = sucPre
	}
	var sucSucList [kSuccessorListSize]NodeInf
	err = node.RemoteCall(suc.Addr, "Node.GetSuccessorList", "", &sucSucList)
	if err != nil {
		logrus.Errorf("[Stabilize] [%s] fail to RemoteCall %s to <GetSuccessorList> err: %s", node.Addr, suc.Addr, err)
		return fmt.Errorf("[Stabilize] [%s] fail to RemoteCall %s to <GetSuccessorList>", node.Addr, suc.Addr)
	}

	node.successorListLock.Lock()
	node.successorList[0] = suc
	for i := 1; i < kSuccessorListSize; i++ {
		node.successorList[i] = sucSucList[i-1]
	}
	node.successorListLock.Unlock()

	node.fingerTableLock.Lock()
	node.fingerTable[0] = suc
	node.fingerTableLock.Unlock()

	var empty string
	err = node.RemoteCall(suc.Addr, "Node.Notify", node.Addr, &empty)
	if err != nil {
		logrus.Errorf("[Stabilize] [%s] fail to RemoteCall %s to <Notify> err: %s", node.Addr, suc.Addr, err)
		return fmt.Errorf("[Stabilize] [%s] fail to RemoteCall %s to <Nofity>", node.Addr, suc.Addr)
	}

	return nil
}

func (node *Node) Notify(arg string, _ *string) error {
	if !node.online {
		logrus.Errorf("[Notify] [%s] is offline", node.Addr)
		return fmt.Errorf("[Notify] [%s] is offline", node.Addr)
	}
	pre := node.getPredecessor()
	var empty string
	if arg == "" {
		if pre.Addr != "" && !node.ping(pre.Addr) {
			node.predecessorLock.Lock()
			node.predecessor = NodeInf{}
			node.predecessorLock.Unlock()
			node.absorbBackups()
			suc := node.getSuccessor()
			err := node.RemoteCall(suc.Addr, "Node.PutBackups", node.backup, &empty)
			if err != nil {
				logrus.Errorf("[Notify] [%s] fail to RemoteCall %s to <PutBackups> err: %s", node.Addr, suc.Addr, err)
				return fmt.Errorf("[Notify] [%s] fail to RemoteCall %s to <PutBackups>", node.Addr, suc.Addr)
			}
			node.backupLock.Lock()
			node.backup = make(map[string]string)
			node.backupLock.Unlock()
		}
	} else {
		if node.ping(arg) && (pre.Addr == "" || Contain(Hash(arg), pre.Identify, node.Identify)) {
			node.predecessorLock.Lock()
			node.predecessor = NodeInf{arg, Hash(arg)}
			node.predecessorLock.Unlock()

			data:=make(map[string]string)
			err := node.RemoteCall(arg, "Node.GetData", "", &data)
			if err != nil {
				logrus.Errorf("[Notify] [%s] fail to RemoteCall %s to <GetData> err: %s", node.Addr, arg, err)
				return fmt.Errorf("[Notify] [%s] fail to RemoteCall %s to <GetData>", node.Addr, arg)
			}
			node.backupLock.Lock()
			node.backup = data
			node.backupLock.Unlock()
		}
	}
	return nil
}

//
// DHT methods for interfaces
//

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
	logrus.Infof("[Run] %s ", node.Addr)
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
	node.onlineLock.Lock()
	defer node.onlineLock.Unlock()
	node.online = true
}

// "Create" or "Join" will be called after calling "Run".
// For a dhtNode, either "Create" or "Join" will be called, but not both.

// Create a new network.
func (node *Node) Create() {
	logrus.Infof("[Create] [%s]", node.Addr)
	nodeId := NodeInf{node.Addr, node.Identify}

	node.successorListLock.Lock()
	node.successorList[0] = nodeId
	node.successorListLock.Unlock()

	node.fingerTableLock.Lock()
	node.fingerTable[0] = nodeId
	node.fingerTableLock.Unlock()

	node.nxtFin = 1
	node.update()
}

// Join an existing network. Return "true" if join succeeded and "false" if not.
func (node *Node) Join(addr string) bool {
	logrus.Infof("[Join] join [%s] to %s", node.Addr, addr)

	node.predecessorLock.Lock()
	node.predecessor = NodeInf{}
	node.predecessorLock.Unlock()

	var suc NodeInf
	err := node.RemoteCall(addr, "Node.FindSuccessor", NodeInf{node.Addr, node.Identify}, &suc)
	if err != nil {
		logrus.Errorf("[Join] [%s] fail to RemoteCall %s to <FindSuccessor> err: %s", node.Addr, addr, err)
		return false
	}

	var sucSucList [kSuccessorListSize]NodeInf
	err = node.RemoteCall(suc.Addr, "Node.GetSuccessorList", "", &sucSucList)
	if err != nil {
		logrus.Errorf("[Join] [%s] fail to RemoteCall %s to <GetSuccessorList> err: %s", node.Addr, addr, err)
		return false
	}

	node.successorListLock.Lock()
	node.successorList[0] = suc
	for i := 1; i < kSuccessorListSize; i++ {
		node.successorList[i] = sucSucList[i-1]
	}
	node.successorListLock.Unlock()

	node.fingerTableLock.Lock() // 全部一起修改可能会死锁
	node.fingerTable[0] = suc
	node.fingerTableLock.Unlock()
	node.nxtFin = 1

	node.dataLock.Lock()
	err = node.RemoteCall(suc.Addr, "Node.TransferData", NodeInf{node.Addr, node.Identify}, &node.data)
	node.dataLock.Unlock()
	if err != nil {
		logrus.Errorf("[Join] [%s] fail to RemoteCall %s to <TransferData> err: %s", node.Addr, suc.Addr, err)
		return false
	}
	logrus.Infof("[Join] [%s] joined successfully", node.Addr)
	node.update()
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
	node.clear()
	var empty string
	suc := node.getSuccessor()
	err = node.RemoteCall(suc.Addr, "Node.Notify", "", &empty)
	if err != nil {
		logrus.Errorf("[Quit] [%s] fail to RemoteCall %s to <Notify> err: %s", node.Addr, suc.Addr, err)
	}
	pre := node.getPredecessor()
	err = node.RemoteCall(pre.Addr, "Node.Stabilize", "", &empty)
	if err != nil {
		logrus.Errorf("[Quit] [%s] fail to RemoteCall %s to <Stabilize> err: %s", node.Addr, suc.Addr, err)
	}
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
	node.clear()
	logrus.Infof("[ForceQuit] [%s] success with %v", node.Addr, node.online)
}

// Check whether the node identified by addr is in the network.
func (node *Node) ping(addr string) bool {
	var empty string
	err := node.RemoteCall(addr, "Node.Ping", "", &empty)
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
	logrus.Infof("[Put] key:%s value:%s", key, value)
	var tar NodeInf
	err := node.FindSuccessor(NodeInf{key, Hash(key)}, &tar)
	if err != nil {
		logrus.Errorf("[Put] [%s] fail to <Findsuccessor> of key:%s err: %s", node.Addr, key, err)
		return false
	}
	var empty string
	err = node.RemoteCall(tar.Addr, "Node.PutValue", Pair{key, value}, &empty)
	if err != nil {
		logrus.Errorf("[Put] [%s] fail to RemoteCall %s to <PutValue> key:%s,value:%s err: %s", node.Addr, tar.Addr, key, value, err)
		return false
	}
	return true
}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
func (node *Node) Get(key string) (bool, string) {
	if !node.online {
		logrus.Errorf("[Get] [%s] is offline", node.Addr)
		return false, ""
	}
	logrus.Infof("[Get] key:%s", key)
	var tar NodeInf
	err := node.FindSuccessor(NodeInf{key, Hash(key)}, &tar)
	if err != nil {
		logrus.Errorf("[Get] [%s] fail to <Findsuccessor> of key:%s err: %s", node.Addr, key, err)
		return false, ""
	}
	var value string
	err = node.RemoteCall(tar.Addr, "Node.GetValue", key, &value)
	if err != nil {
		logrus.Errorf("[Get] [%s] fail to RemoteCall %s to <GetValue> key:%s err: %s", node.Addr, tar.Addr, key, err)
		return false, ""
	}
	return true, value
}

// Remove a key-value pair identified by KEY from the network.
// Return "true" if success, "false" otherwise.
func (node *Node) Delete(key string) bool {
	if !node.online {
	logrus.Errorf("[Delete] [%s] is offline", node.Addr)
	return false
	}
	logrus.Infof("[Delete] key:%s", key)
	var tar NodeInf
	err := node.FindSuccessor(NodeInf{key, Hash(key)}, &tar)
	if err != nil {
		logrus.Errorf("[Delete] [%s] fail to <Findsuccessor> of key:%s err: %s", node.Addr, key, err)
		return false
	}
	var empty string
	err = node.RemoteCall(tar.Addr, "Node.DeleteValue", key, &empty)
	if err != nil {
		logrus.Errorf("[Delete] [%s] fail to RemoteCall %s to <DeleteValue> key:%s err: %s", node.Addr, tar.Addr, key, err)
		return false
	}
	return true
}

// DHT methods for assistant
func (node *Node) fixFingers() {
	finStart := HashFinger(node.Identify, node.nxtFin)
	var finNode NodeInf
	err := node.FindSuccessor(NodeInf{"", finStart}, &finNode)
	if err != nil {
		logrus.Errorf("[Join] [%s] fail to <Findsuccessor> err: %s", node.Addr, err)
		return
	} else {
		node.fingerTableLock.Lock()
		node.fingerTable[node.nxtFin] = finNode
		node.fingerTableLock.Unlock()
	}
	node.nxtFin = (node.nxtFin + 1) % kFingerTableSize
}

func (node *Node) getPredecessor() NodeInf {
	node.predecessorLock.RLock()
	defer node.predecessorLock.RUnlock()
	return node.predecessor
}

func (node *Node) getSuccessor() NodeInf {
	for i := 0; i < kSuccessorListSize; i++ {
		node.successorListLock.RLock()
		suc := node.successorList[i]
		node.successorListLock.RUnlock()
		if node.ping(suc.Addr) {
			// logrus.Infof("[getSuccessor] get [%s]'s successor %s", node.Addr, suc.Addr)
			return suc
		}
	}
	logrus.Errorf("[getSuccessor] fail to find the successor of [%s]", node.Addr)
	return NodeInf{}
}

// func (node *Node) findPredecessor(id *big.Int) NodeInf {
// 	node.fingerTableLock.RLock()
// 	defer node.fingerTableLock.RUnlock()
// 	node_tmp := node
// 	for !Contain(id, node_tmp.identify, node_tmp.fingerTable[0].identify) && id != node_tmp.fingerTable[0].identify {
// 		var tmp *Node
// 		tmp.Addr = node_tmp.closestPrecedingFinger(id)
// 		tmp.identify = Hash(tmp.Addr)
// 		node_tmp = tmp
// 	}
// 	return node_tmp.Addr
// }

func (node *Node) closestPrecedingFinger(id *big.Int) NodeInf {
	for i := kFingerTableSize - 1; i >= 0; i-- {
		node.fingerTableLock.RLock()
		fin := node.fingerTable[i]
		node.fingerTableLock.RUnlock()
		if fin.Addr == "" {
			continue
		}
		if !node.ping(fin.Addr) {
			node.fingerTableLock.Lock()
			node.fingerTable[i] = NodeInf{}
			node.fingerTableLock.Unlock()
			continue
		}
		if Contain(fin.Identify, node.Identify, id) {
			return NodeInf{fin.Addr, Hash(fin.Addr)}
		}
	}
	return NodeInf{node.Addr, node.Identify}
}

func (node *Node) absorbBackups() {
	if !node.online {
		logrus.Errorf("[AbsorbBackups] [%s] is offline", node.Addr)
	}
	node.dataLock.Lock()
	node.backupLock.Lock()
	for key, value := range node.backup {
		_, ok := node.data[key]
		if ok {
			logrus.Errorf("[AbsorbBackups] [%s] already have %s in data", node.Addr, key)
		} else {
			node.data[key] = value
		}
	}
	node.dataLock.Unlock()
	node.backupLock.Unlock()
}

func (node *Node) clear() {
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
}

func (node *Node) update() {
	var empty string
	go func() {
		for node.online {
			node.Stabilize("", &empty)
			time.Sleep(updateInterval)
		}
	}()
	go func() {
		for node.online {
			node.fixFingers()
			time.Sleep(updateInterval)
		}
	}()
	go func() {
		for node.online {
			node.Notify("", &empty)
			time.Sleep(updateInterval)
		}
	}()
}