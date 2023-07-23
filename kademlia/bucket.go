package kademlia

import (
	"sync"
)

type ListNode struct {
	Addr string
	Prev *ListNode
	Next *ListNode
}

type List struct{
	NodeAddr string
	Head     *ListNode
	Rear     *ListNode
	Size     int
}

func (l *List) Init(addr string) {
	l.NodeAddr = addr
	l.Head = new(ListNode)
	l.Rear = new(ListNode)
	l.Head.Prev = nil
	l.Head.Next = l.Rear
	l.Rear.Prev = l.Head
	l.Rear.Next = nil
	l.Size = 0
}

func (l *List) pushback(addr string) {
	newNode := ListNode{addr, l.Rear.Prev, l.Rear}
	l.Rear.Prev.Next = &newNode
	l.Rear.Prev = &newNode
	l.Size++
}

func (l *List) popback() {
	point :=l.Rear.Prev
	l.Rear.Prev = l.Rear.Prev.Prev
	l.Rear.Prev.Next = l.Rear
	point.Prev=nil
	point.Next=nil
	l.Size--
}

func (l *List) find(addr string) *ListNode {
	point := l.Head.Next
	for point != l.Rear {
		if point.Addr == addr {
			return point
		}
		point = point.Next
	}
	return nil
}

func (l *List) insert(point *ListNode, addr string) {
	if point == l.Rear || point == nil || point.Next == nil {
		return
	}
	newNode := ListNode{addr, point, point.Next}
	point.Next.Prev = &newNode
	point.Next = &newNode
	l.Size++
}

func (l *List) delete(point *ListNode) {
	if point == l.Head || point == l.Rear || point == nil || point.Next == nil || point.Prev == nil {
		return
	}
	point.Prev.Next = point.Next
	point.Next.Prev = point.Prev
	point.Prev = nil
	point.Next = nil
	l.Size--
}

func (l *List) moveToRear(point *ListNode) {
	addr := point.Addr
	l.delete(point)
	l.pushback(addr)
}

func (l *List) update(addr string) {
	if addr == "" || addr == l.NodeAddr {
		return
	}
	point := l.find(addr)
	if point != nil {
		l.moveToRear(point)
	} else {
		if l.Size < k {
			l.pushback(addr)
		} else {
			var n Node
			if n.Ping(l.Head.Next.Addr) {
				l.delete(l.Head.Prev)
				l.pushback(addr)
			} else {
				l.moveToRear(l.Head.Next)
			}
		}
	}
}

type Bucket struct {
	list List
	mu       sync.RWMutex
}

func (b *Bucket) Init(addr string) {
	b.mu.Lock()
	b.list.Init(addr)
	b.mu.Unlock()
}

// func (b *Bucket) pushback(addr string) {
// 	b.mu.Lock()
// 	b.list.pushback(addr)
// 	b.mu.Unlock()
// }

// func (b *Bucket) popback() {
// 	b.mu.Lock()
// 	b.list.popback()
// 	b.mu.Unlock()
// }
// func (b *Bucket) find(addr string) *ListNode {
// 	b.mu.RLock()
// 	defer b.mu.RUnlock()
// 	return b.list.find(addr)
// }

// func (b *Bucket) insert(point *ListNode, addr string) {
// 	b.mu.Lock()
// 	defer b.mu.Unlock()
// 	b.list.insert(point,addr)
// }

// func (b *Bucket) delete(point *ListNode) {
// 	b.mu.Lock()
// 	defer b.mu.Unlock()
// 	b.list.delete(point)
// }

// func (b *Bucket) moveToRear(point *ListNode) {
// 	b.mu.Lock()
// 	defer b.mu.Unlock()
// 	b.list.moveToRear(point)
// }

func (b *Bucket) update(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.list.update(addr)
}

// 结果列表
type RetList struct {
	Rlist   List // list里的NodeAddr存target
}

func (retList *RetList) insert(addr string) bool {
	var n Node
	if !n.Ping(addr) {
		return false
	}
	point := retList.Rlist.find(addr)
	if point != nil {
		return false
	}
	dis := Xor(Hash(addr), Hash(retList.Rlist.NodeAddr))
	if retList.Rlist.Size < k {
		p := retList.Rlist.Head.Next
		for p != retList.Rlist.Rear {
			oriDis := Xor(Hash(p.Addr), Hash(retList.Rlist.NodeAddr))
			if dis.Cmp(&oriDis) < 0 {
				retList.Rlist.insert(p.Prev, addr)
				return true
			}
			p = p.Next
		}
		retList.Rlist.pushback(addr)
		return true
	}
	p := retList.Rlist.Head.Next
	for p != retList.Rlist.Rear {
		oriDis := Xor(Hash(p.Addr), Hash(retList.Rlist.NodeAddr))
		if dis.Cmp(&oriDis) < 0 {
			retList.Rlist.popback()
			retList.Rlist.insert(p.Prev, addr)
			return true
		}
		p = p.Next
	}
	return false
}

func (retList *RetList) delete(addr string) bool{
	p := retList.Rlist.Head.Next
	for p != retList.Rlist.Rear {
		if addr==p.Addr {
			retList.Rlist.delete(p)
			return true
		}
		p = p.Next
	}
	return false
}
