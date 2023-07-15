# DHT-2023 Report

## Chord
### Details
- 每个节点存储的predecessor和successorList保证正确性的同时能帮助维护环在Quit和ForceQuit时不会断裂，fingerTable大大提高了Chord的查找效率
- 将 Notify() 分成两种情况。一种传入地址，表示通知另一节点自己的存在，另一种传入空字符串，通知另一节点自行检查自己的前继
- fixFingers() 不采用random index，而是在每个节点记录下一次要fix的位置，逐次递增
### Debug
调试过程中遇到了两个主要问题。  
1.在运行的任何时候都有可能因为oom（out of memory）被程序强行中断。经过检查发现是计算fingertable时未取余   
2.Quit函数进行完之后远程调用一直会出现dial error的错误。先参考同组同学的方法调整了Run()并行部分的结构，但没有效果。最后发现是对Ping()的理解有问题，应当为接收远程调用对方使用Ping函数后返回的error变量  

打印出的包含足够信息的log可以帮助调试

### File Structure
chord/node.go: 包含了实现Chord的主体，包括rpc远程调用、实现userdef.go接口等部分  
chord/utils.go: 定义了工具函数和全局变量
