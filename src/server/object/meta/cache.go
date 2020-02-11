package meta

import (
	"sync"
)

type MetaCache struct {
	mutex    sync.RWMutex
	metaList map[string]*Meta
}

func (mc *MetaCache) Get(metaName string) *Meta {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()
	if meta, ok := mc.metaList[metaName]; ok {
		return meta
	} else {
		return nil
	}
}

func (mc *MetaCache) GetList() []*Meta {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()
	if len(mc.metaList) > 0 {
		metaList := make([]*Meta, 0)
		for _, meta := range mc.metaList {
			metaList = append(metaList, meta)
		}
		return metaList
	}
	return nil
}

func (mc *MetaCache) Set(meta *Meta) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.metaList[meta.Name] = meta
}

func (mc *MetaCache) Invalidate() {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.metaList = make(map[string]*Meta, 0)
}

func NewCache() *MetaCache {
	return &MetaCache{mutex: sync.RWMutex{}, metaList: make(map[string]*Meta, 0)}
}
