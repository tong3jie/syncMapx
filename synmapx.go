package syncmapx

import (
	"encoding/json"
	"fmt"
	"sync"
)

var SHARD_COUNT = 32

type Stringer interface {
	fmt.Stringer
	comparable
}

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type ConcurrentMap[K comparable, V any] struct {
	smap *sync.Map
}

func create[K comparable, V any]() ConcurrentMap[K, V] {
	m := ConcurrentMap[K, V]{
		smap: &sync.Map{},
	}
	return m
}

// Creates a new concurrent map.
func New[V any]() ConcurrentMap[string, V] {
	return create[string, V]()
}

// Creates a new concurrent map.
func NewStringer[K Stringer, V any]() ConcurrentMap[K, V] {
	return create[K, V]()
}

// Creates a new concurrent map.
func NewWithCustomShardingFunction[K comparable, V any]() ConcurrentMap[K, V] {
	return create[K, V]()
}

func (m ConcurrentMap[K, V]) MSet(data map[K]V) {
	for key, value := range data {
		m.smap.Store(key, value)
	}
}

// Sets the given value under the specified key.
func (m ConcurrentMap[K, V]) Set(key K, value V) {
	m.smap.Store(key, value)
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb[V any] func(exist bool, valueInMap V, newValue V) V

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcurrentMap[K, V]) Upsert(key K, value V, cb UpsertCb[V]) (res V) {
	var val V
	v, ok := m.smap.Load(key)
	if valueNew, ok1 := v.(V); ok1 {
		val = valueNew
	}
	res = cb(ok, val, value)
	m.smap.Store(key, res)
	return res
}

// Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentMap[K, V]) SetIfAbsent(key K, value V) bool {
	// Get map shard.

	_, ok := m.smap.Load(key)
	if !ok {
		m.smap.Store(key, value)
	}
	return !ok
}

// Get retrieves an element from map under given key.
func (m ConcurrentMap[K, V]) Get(key K) (V, bool) {
	// Get shard
	var value V
	val, ok := m.smap.Load(key)
	if valueNew, ok1 := val.(V); ok1 {
		return valueNew, ok
	}
	return value, false
}

// Count returns the number of elements within the map.
func (m ConcurrentMap[K, V]) Count() int {
	count := 0
	m.smap.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}

// Looks up an item under specified key
func (m ConcurrentMap[K, V]) Has(key K) bool {
	_, ok := m.smap.Load(key)
	return ok
}

// Remove removes an element from the map.
func (m ConcurrentMap[K, V]) Remove(key K) {
	m.smap.Delete(key)
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCb[K any, V any] func(key K, v V, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m ConcurrentMap[K, V]) RemoveCb(key K, cb RemoveCb[K, V]) bool {
	// Try to get shard.
	var value V
	v, ok := m.smap.Load(key)
	if valueNew, ok1 := v.(V); ok1 {
		value = valueNew
	}
	remove := cb(key, value, ok)
	if remove && ok {
		m.smap.Delete(key)
	}
	return remove
}

// Pop removes an element from the map and returns it
func (m ConcurrentMap[K, V]) Pop(key K) (v V, exists bool) {
	var value V
	val, exists := m.smap.Load(key)
	if valueNew, ok1 := val.(V); ok1 {
		m.smap.Delete(key)
		return valueNew, exists
	}
	return value, exists
}

// IsEmpty checks if map is empty.
func (m ConcurrentMap[K, V]) IsEmpty() bool {
	return m.Count() == 0
}

// Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple[K comparable, V any] struct {
	Key K
	Val V
}

// Iter returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m ConcurrentMap[K, V]) Iter() <-chan Tuple[K, V] {
	ch := make(chan Tuple[K, V])
	count := 0
	mcount := m.Count()
	go func() {
		m.smap.Range(func(key, value any) bool {
			ch <- Tuple[K, V]{key.(K), value.(V)}
			count++
			if count == mcount {
				close(ch)
			}
			return true
		})
	}()
	return ch
}

// Clear removes all items from map.
func (m ConcurrentMap[K, V]) Clear() {
	for item := range m.Iter() {
		m.Remove(item.Key)
	}
}

// Items returns all items as map[string]V
func (m ConcurrentMap[K, V]) Items() map[K]V {
	tmp := make(map[K]V)

	// Insert items to temporary map.
	for item := range m.Iter() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// Iterator callbacalled for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb[K comparable, V any] func(key K, v V)

// Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcurrentMap[K, V]) IterCb(fn IterCb[K, V]) {
	m.smap.Range(func(key, value any) bool {
		fn(key.(K), value.(V))
		return true
	})

}

// Keys returns all keys as []string
func (m ConcurrentMap[K, V]) Keys() []K {
	count := m.Count()
	i := 0
	ch := make(chan K, count)
	// Foreach shard.
	go func() {
		m.smap.Range(func(key, value any) bool {
			ch <- key.(K)
			i++
			if i == count {
				close(ch)
			}
			return true
		})

	}()

	// Generate keys
	keys := make([]K, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// Reviles ConcurrentMap "private" variables to json marshal.
func (m ConcurrentMap[K, V]) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across shards.
	tmp := make(map[K]V)

	// Insert items to temporary map.
	for item := range m.Iter() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

// Reverse process of Marshal.
func (m *ConcurrentMap[K, V]) UnmarshalJSON(b []byte) (err error) {
	tmp := make(map[K]V)

	// Unmarshal into a single map.
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	// foreach key,value pair in temporary map insert into our concurrent map.
	for key, val := range tmp {
		m.Set(key, val)
	}
	return nil
}
