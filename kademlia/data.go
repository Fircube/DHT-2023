package kademlia

import (
	"sync"
	"time"
)

type Pair struct {
	Key   string
	Value string
}

type DataType struct {
	Data          map[string]string
	ExpireTime    map[string]time.Time // 过期时间
	RepublishTime map[string]time.Time // 重新发布时间
	mu            sync.RWMutex
}

func (data *DataType) Init() {
	data.mu.Lock()
	data.Data = make(map[string]string)
	data.ExpireTime = make(map[string]time.Time)
	data.RepublishTime = make(map[string]time.Time)
	data.mu.Unlock()
}

func (data *DataType) put(key string, value string) {
	data.mu.Lock()
	data.Data[key] = value
	data.ExpireTime[key] = time.Now().Add(ExpireTime)
	data.RepublishTime[key] = time.Now().Add(RepublishTime)
	data.mu.Unlock()
}

func (data *DataType) get(key string) (bool, string) {
	data.mu.RLock()
	value, ok := data.Data[key]
	data.mu.RUnlock()
	return ok, value
}

func (data *DataType) republish() map[string]string {
	republishList:=make(map[string]string)
	data.mu.RLock()
	for key, t := range data.RepublishTime {
		if time.Now().After(t) {
			republishList[key] = data.Data[key]
		}
	}
	data.mu.RUnlock()
	return republishList
}

func (data *DataType) expire() {
	var invalid []string
	data.mu.RLock()
	for key, t := range data.ExpireTime {
		if time.Now().After(t) {
			invalid = append(invalid, key)
		}
	}
	data.mu.RUnlock()
	data.mu.Lock()
	for _, k := range invalid {
		delete(data.Data, k)
		delete(data.ExpireTime, k)
		delete(data.RepublishTime, k)
	}
	data.mu.Unlock()
}

func (data *DataType) clear() {
	data.mu.Lock()
	data.Data = make(map[string]string)
	data.ExpireTime = make(map[string]time.Time)
	data.RepublishTime = make(map[string]time.Time)
	data.mu.Unlock()
}
