// Package mr 线程安全的map
package mr

import (
	"sync"
)

type ThreadSafetyMap struct {
	hash map[int]Task
	lock sync.Mutex
}

func NewThreadSafetyMap() ThreadSafetyMap {
	hash := make(map[int]Task, 0)
	lock := sync.Mutex{}
	return ThreadSafetyMap{
		hash: hash,
		lock: lock,
	}
}

func (t *ThreadSafetyMap) Set(key int, value Task) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.hash[key] = value
}

func (t *ThreadSafetyMap) Get(key int) Task {
	t.lock.Lock()

	defer t.lock.Unlock()

	return t.hash[key]
}

// Delete 删除map中的某个元素
func (t *ThreadSafetyMap) Delete(key int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.hash, key)
}

func (t *ThreadSafetyMap) Size() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.hash)
}
