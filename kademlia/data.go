package kademlia

import (
	"sync"
	"time"
)

type Pair struct {
	key   string
	value string
}

type Data struct {
	data          map[string]string
	expireTime    map[string]time.Time // 过期时间
	republishTime map[string]time.Time // 重新发布时间
	mu            sync.RWMutex
}

func (data *Data) Init() {
	data.mu.Lock()
	data.data = make(map[string]string)
	data.expireTime = make(map[string]time.Time)
	data.republishTime = make(map[string]time.Time)
	data.mu.Unlock()
}

func (data *Data) put(key string, value string) {
	data.mu.Lock()
	data.data[key] = value
	data.expireTime[key] = time.Now().Add(ExpireTime)
	data.republishTime[key] = time.Now().Add(RepublishTime)
	data.mu.Unlock()
}

func (data *Data) get(key string) (bool, string) {
	data.mu.RLock()
	value, ok := data.data[key]
	data.mu.RUnlock()
	return ok, value
}

func (data *Data) republish() []string {
	var republishList []string
	data.mu.RLock()
	for k, republishTime := range data.republishTime {
		if time.Now().After(republishTime) {
			republishList = append(republishList, k)
		}
	}
	data.mu.RUnlock()
	return republishList
}

func (data *Data) expire() {
	var invalid []string
	data.mu.Lock()
	for k, expireTime := range data.expireTime {
		if time.Now().After(expireTime) {
			invalid = append(invalid, k)
		}
	}
	for _, k := range invalid {
		delete(data.data, k)
		delete(data.expireTime, k)
		delete(data.republishTime, k)
	}
	data.mu.Unlock()
}

func (data *Data) clear() {
	data.mu.Lock()
	data.data = make(map[string]string)
	data.expireTime = make(map[string]time.Time)
	data.republishTime = make(map[string]time.Time)
	data.mu.Unlock()
}
