package kademlia

import (
	"sync"
)

type List struct {
	LocalAddr string
	PeerAddr  [k]string
	Size      int
}

func (l *List) Init(addr string) {
	l.LocalAddr = addr
	l.Size = 0
}

func (l *List) pushback(addr string) {
	l.PeerAddr[l.Size] = addr
	l.Size++
}

func (l *List) popback() {
	l.Size--
	l.PeerAddr[l.Size] = ""
}

func (l *List) find(addr string) int {
	for i := 0; i < l.Size; i++ {
		if l.PeerAddr[i] == addr {
			return i
		}
	}
	return -1
}

func (l *List) insert(ind int, addr string) {
	if ind >= k {
		return
	}
	for i := l.Size; i > ind; i-- {
		l.PeerAddr[i] = l.PeerAddr[i-1]
	}
	l.PeerAddr[ind] = addr
	l.Size++
}

func (l *List) delete(ind int) {
	if ind >= l.Size {
		return
	}
	for i := ind; i < l.Size-1; i++ {
		l.PeerAddr[i] = l.PeerAddr[i+1]
	}
	l.PeerAddr[l.Size-1] = ""
	l.Size--
}

func (l *List) moveToRear(ind int) {
	addr := l.PeerAddr[ind]
	l.delete(ind)
	l.pushback(addr)
}

func (l *List) update(addr string) {
	if addr == "" {
		return
	}
	point := l.find(addr)
	if point != -1 {
		l.moveToRear(point)
	} else {
		if l.Size < k {
			l.pushback(addr)
		} else {
			var n Node
			if n.Ping(l.PeerAddr[0]) {
				l.delete(0)
				l.pushback(addr)
			} else {
				l.moveToRear(0)
			}
		}
	}
}

type Bucket struct {
	list List
	mu   sync.RWMutex
}

func (b *Bucket) Init(addr string) {
	b.mu.Lock()
	b.list.Init(addr)
	b.mu.Unlock()
}

func (b *Bucket) update(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.list.update(addr)
}

func (b *Bucket) delete(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i < b.list.Size; i++ {
		if addr == b.list.PeerAddr[i] {
			b.list.delete(i)
			return
		}
	}
}

// 结果列表
type RetList struct {
	List // list里的LocalAddr存target
}

func (retList *RetList) Insert(addr string) bool {
	var n Node
	if !n.Ping(addr) {
		return false
	}
	point := retList.find(addr)
	if point != -1 {
		return false
	}
	dis := Xor(Hash(addr), Hash(retList.LocalAddr))
	if retList.Size < k {
		for i := 0; i < retList.Size; i++ {
			oriDis := Xor(Hash(retList.PeerAddr[i]), Hash(retList.LocalAddr))
			if dis.Cmp(&oriDis) < 0 {
				retList.insert(i, addr)
				return true
			}
		}
		retList.pushback(addr)
		return true
	} else {
		for i := 0; i < retList.Size; i++ {
			oriDis := Xor(Hash(retList.PeerAddr[i]), Hash(retList.LocalAddr))
			if dis.Cmp(&oriDis) < 0 {
				retList.popback()
				retList.insert(i, addr)
				return true
			}
		}
		return false
	}

}

func (retList *RetList) Delete(addr string) bool {
	for i := 0; i < retList.Size; i++ {
		if addr == retList.PeerAddr[i] {
			retList.delete(i)
			return true
		}
	}
	return false
}
