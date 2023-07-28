# DHT-2023 Report

## **Chord**
### Details
- 每个节点存储的predecessor和successorList保证正确性的同时能帮助维护环在Quit和ForceQuit时不会断裂，fingerTable大大提高了Chord的查找效率
- 将 Notify() 分成两种情况。一种传入地址，表示通知另一节点自己的存在，另一种传入空字符串，通知另一节点自行检查自己的前继
- fixFingers() 不采用random index，而是在每个节点记录下一次要fix的位置，逐次更新

### Debug
1. fatal error: oom（out of memory） 产生原因：计算fingertable时未取余   
2. fatal error: i/o timeout 
    产生原因：模拟高并发的场景，会出现批量的`TIME_WAIT`的`TCP`连接，在2个`MSL`时间内建立、关闭超过65535个`TCP`连接就会出现端口耗尽问题。 
    解决方法：`TCP`连接复用/换用`UDP`协议
3. 结构体内部指针不应直接赋值。 `big.Int`类复制值必须使用`Int.Set`方法；浅拷贝不受支持且可能会导致错误
4. 打印出的包含足够信息的log可以帮助调试

### File Structure
- **`chord/node.go`**: 包含了实现Chord的主体，包括rpc远程调用、实现userdef.go接口等部分  
- **`chord/utils.go`**: 定义了哈希工具函数和全局变量

## **Kademlia**
### Details
- 选用异或衡量距离划分`bucket`，将路由查找和存储等操作复杂度降到对数级别
- 数据备份更多，提高数据的可靠性和可用性。但由于没有删除操作，存储副本的数量越多，添加数据所需的时间和网络开销也会相应增加。

### Debug
1. rpc调用传参相关问题。
    实现`bucket`的时候一开始使用链表写法，因为结构体内含指针，出现循环引用问题。`rpc`调用会对参数进行`gob encoding`,当使用`encoding`包对这样的结构体进行序列化时，会递归地对`encode`指针指向的对象进行，可能会导致堆栈溢出错误。
2. `kademlia`中的时间参数与程序运行时间和性能、数据冲突竞争或丢失息息相关，需要反复调试
3. 在`Republish()`和`Store()`等函数中适当并发能极大提高效率

### File Structure
- **`kademlia/bucket.go`**:
- **`kademlia/data.go`**: 
- **`kademlia/node.go`**: 包含了实现Kademlia的主体，包括rpc远程调用、实现userdef.go接口等部分  
- **`kademlia/utils.go`**: 定义了哈希工具函数和全局变量

