package chunkserver

import (
	"gdfs/types"
	"sync"
	"time"
)

type cacheItem struct {
	data   []byte
	expire time.Time
}

type CacheBuffer struct {
	sync.RWMutex
	buffer map[types.DataBufferID]cacheItem
	expire time.Duration
	tick   time.Duration
}

// newCacheBuffer returns a CacheBuffer. Default expire time is expire.
// The CacheBuffer will cleanup expired items every tick.
func newCacheBuffer(expire, tick time.Duration) *CacheBuffer {
	buf := &CacheBuffer{
		buffer: make(map[types.DataBufferID]cacheItem),
		expire: expire,
		tick:   tick,
	}

	// cleanup
	go func() {
		ticker := time.NewTicker(tick)
		for {
			<-ticker.C
			now := time.Now()
			buf.Lock()
			for id, item := range buf.buffer {
				if item.expire.Before(now) {
					delete(buf.buffer, id)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

// allocate a new DataID for given handle
func NewDataID(handle types.ChunkHandle) types.DataBufferID {
	now := time.Now()
	timeStamp := now.Nanosecond() + now.Second()*1000 + now.Minute()*60*1000
	return types.DataBufferID{
		Handle:    handle,
		TimeStamp: timeStamp,
	}
}

func (buf *CacheBuffer) Set(id types.DataBufferID, data []byte) {
	buf.Lock()
	defer buf.Unlock()
	buf.buffer[id] = cacheItem{data, time.Now().Add(buf.expire)}
}

func (buf *CacheBuffer) Get(id types.DataBufferID) ([]byte, bool) {
	buf.Lock()
	defer buf.Unlock()
	item, ok := buf.buffer[id]
	if !ok {
		return nil, ok
	}
	item.expire = time.Now().Add(buf.expire) // touch
	return item.data, ok
}

func (buf *CacheBuffer) Fetch(id types.DataBufferID) ([]byte, error) {
	buf.Lock()
	defer buf.Unlock()

	item, ok := buf.buffer[id]
	if !ok {
		return nil, types.ErrNotFound
	}

	delete(buf.buffer, id)
	return item.data, nil
}

func (buf *CacheBuffer) Delete(id types.DataBufferID) {
	buf.Lock()
	defer buf.Unlock()
	delete(buf.buffer, id)
}
