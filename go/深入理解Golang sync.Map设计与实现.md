导语 | Golang 为并发编程提供了多种并发原语(Mutex、 [RWMutex](https://zhida.zhihu.com/search?content_id=230667068&content_type=Article&match_order=1&q=RWMutex&zhida_source=entity) 、sync.Map)，用于临界区的数据访问和保护；开发应用时，面对不同的场景如何选择合适的并发原语，使功能正常实现的同时提供更高的性能；在互联网应用中，由并发原语保护的临界区从本质上来说无非三种情况：读多写少、写多读少、读写一致。 上篇文章介绍了 sync.RWMutex并发原语，其适用于写多读少的场景；Mutex、RWMutex 其保护的都是某个临界状态区，无论是访问、存储都使用锁进行保护；当操作频繁且要求性能的情况下，锁的优化已无法满足业务需求，此时就需要考虑额外的方式来提升性能。对于读多写少的场景，最常用的方式是进行读写分离，将访问压力分散至其它组件。

Golang为了支持读多写少的场景，提供了sync.Map并发原语，由普通map、Mutex与原子变量组合而成，作为一个并发安全的map，部分情况下读、写数据通过原子操作避免加锁，从而提高临界区访问的性能，同时在高并发的情况下仍能保证数据的准确性，支持Load、Store、 Delete、 Range等操作，可以实现对sync.Map的遍历以及根据key获取value；sync.Map可以有效地替代锁的使用，提高程序的执行效率。

## 设计原理

讨论设计原理前，先观察下sync.Map的结构定义，主要三种元素组成：map、mutex、atomic

```go
type Map struct {
    mu Mutex
    read atomic.Value // readOnly
    dirty map[interface{}]*entry
    misses int
}

// readOnly is an immutable struct stored atomically in the Map.read field.
type readOnly struct {
    m       map[interface{}]*entry
    amended bool
}
```

[sync.Map](https://link.zhihu.com/?target=http%3A//sync.map/) 采用了装饰器 模式，在普通的map上组合其它结构进行修饰，在利用map快速定位某个元素的同时增加了读写分离的功能，当元素在只读组件read中时，使用原子操作避免加锁快速访问其数据，在sync.Map中新增元素或访问不存在的元素时操作dirty组件时使用锁操作进行保护，当只读组件read的命中率降低至一定阈值时触发状态转换，将dirty状态提升为最新的read状态，以降低访问sync.Map时加锁的次数。

下面将介绍sync.Map中每个状态字段的功能及其含义

1. read 提供读写分离的读功能，使用atomic.Value原子操作提供并发的能力，当数据在read中时原子操作可以避免加锁提供并发访问的能力；当read的命中率降低到一定阈值时，触发将脏数据dirty更新为read，此时需要加锁保护。read实际使用map存储数据，它存储的\*entry字段值可以使用CAS操作并发更新，且该\*entry与dirty中存储的值指向同一地址，因此CAS修改后操作结果，read与dirty都可以观察到。
1. Map.read的原子状态中实际存储的结构为readOnly，它是一个“只读数据”，此处的只读是指readOnly.m的值不会修改，而不是说它存储的key的value值不会修改；因此数据库概念中理解的读写分离不同。
	2. 其中m存储k-v数据，是dirty的一个子集，注意的是：它存储的某个key的数据值与dirty中该key存储的值指向同一个地址，因此当修改read中某个key的值时，dirty中该key的值也会同步修改；
	3. amended 表示dirty中是否含有readOnly.m不存在的数据.

2\. dirty 提供读写分离的写功能，是一个非线程安全的map，用Mutex锁进行保护。包含所有key的数据，其中key可以分为两类，仅存在于dirty的新数据、read与dirty中都存在的数据。当查询某个key时，如果read中不存在，且其amended状态为true，表示dirty中存在read未包含的数据，需要再查询dirty中是否包含该key.

3\. mu 互斥锁保护 read、dirty，在两种情况下需要加锁对sync.Map的状态进行保护；1. 从dirty中读写数据时，需要使用互斥锁保护，避免并发操作导致非线程安全的map触发panic；2. 从read状态更新开始累计统计加锁操作的数量misses（即命中read状态失败的次数），如果命中失败的次数大于dirty中元素的数量时，sync.Map的命中率太低需要将dirty提升为read，提升过程中使用锁进行保护。

4\. misses 累计从read更新后进行加锁操作的数量（即read状态命中失败的次数）（例如：当查询key时，如果read中未找到，需要加锁查询dirty；或向 dirty中写入数据需要加锁保护），当misses值大于等于dirty中包含的元素数量时，将dirty提升为read，并将dirty置为nil.

## 功能实现

sync.Map提供了Load、Store、Delete、Range基础操作，以及 [LoadAndDelete](https://zhida.zhihu.com/search?content_id=230667068&content_type=Article&match_order=1&q=LoadAndDelete&zhida_source=entity) 、LoadOrStore复合操作，下面将介绍sync.Map如何实现这些功能、以及在读多写少情况下提供高性能的原因。

### Load 读取操作

1. 从read只读的原子状态中查找，避免加锁，如果存在，则返回数据；如果不存在，且amended为false，表示dirty未包含read中不存在的数据，直接返回.
1. 快速路径，使用原子操作，避免加锁
3. read中不存在且amended为true（表示dirty中含有read不存在的数据），进入慢路径，查询dirty；此时先上锁，再次查询read，防止在获取互斥锁期间 dirty提升为read；仍然不存在且amended仍为true，则查寻dirty，无论是否找到，都调用 [missLocked](https://zhida.zhihu.com/search?content_id=230667068&content_type=Article&match_order=1&q=missLocked&zhida_source=entity) 将统计值misses递增，表示使用了一次互斥锁。
```
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
    // 1. 先查询只读的原子状态
    read, _ := m.read.Load().(readOnly)
    e, ok := read.m[key]
    // 2. 不存在，且amended为true，进入慢路径
    if !ok && read.amended {
        // 2.1 获取互斥锁
        m.mu.Lock()
        // 2.2 再次查询只读的原子状态，存在获取互斥锁期间read状态被更新的情况
        read, _ = m.read.Load().(readOnly)
        e, ok = read.m[key]
        // 2.3 仍然不存在，查询dirty
        if !ok && read.amended {
            e, ok = m.dirty[key]
            // 2.4 将统计数据累加，达到条件时，将dirty提升为read
            m.missLocked()
        }
        m.mu.Unlock()
    }
    if !ok {
        return nil, false
    }
    // 返回存储的数据
    return e.load()
}
```

missLocked 有两个功能：

1. 累计read更新后命中丢失的次数
2. 当命中丢失次数达到阈值时，将dirty提升为read，并将dirty置为nil，misses状态清0；将dirty提升read后，提升下次只读状态read查询的命中率，避免加锁。
```
func (m *Map) missLocked() {
     m.misses++
     if m.misses < len(m.dirty) {
         return
     }
     m.read.Store(readOnly{m: m.dirty})
     m.dirty = nil
     m.misses = 0
}
```

entry.load: sync.Map的 read、dirty中存储数据时，实际用户的数据是存储在entry.p中，存储了用户对象的地址，当从read或dirty查询到对象时，需要使用load 方法加载出用户对象。

如果entry.p为nil或expunged，则表示用户对象已被删除。

```
type entry struct {
     p unsafe.Pointer // *interface{}
}
func (e *entry) load() (value interface{}, ok bool) {
     p := atomic.LoadPointer(&e.p)

     if p == nil || p == expunged {
        return nil, false
     }
     return *(*interface{})(p), true
}
```

entry.p 存储值逻辑上有3种状态，value（存储了用户对象）、nil、expunged.

1. value: 存储了用户对象，允许从一个old\_value更新到new\_value
2. nil: 当删除对应的key时，不会立即将key从sync.Map中删除，而是将它的值设为nil，表示逻辑上删除；当map运行过程中，从只读的read状态构建写状态dirty时，对于值为nil的key，将其设置为expunged态，对于expunged态的key不写入dirty，当下次dirty提升为read状态时，expunged态的key由于未被任何状态记录从而被统一删除。
3. expunged: 将一个key标识为擦除，处于该状态的key只存在于read状态中，dirty中不存在；因此当存储一个key对应的值时，如果它key对应的状态为擦除态，需要先将其修改为nil添加到dirty中，才可以更新entry对应的p值；如果直接由expunged更新为new\_value，会导致read与dirty状态不一致。
1. 一个key的删除，从逻辑上立即需要两轮 dirty->read的提升才会被真正删除；在删除key后的第一次dirty->read提升，将key的值从nil -> expunged；在第二轮将标记为expunged值丢弃由GC回收.
![](https://pic3.zhimg.com/v2-271ecfacd93307691030f765eff52442_1440w.jpg)

### Store 存储操作

将一个新对象记录到sync.Map或更新已有的对象，对于不同的对象，使用不同的方式提供执行效率。

1. 更新一个存在于read状态中的非擦除对象时，使用CAS原子操作避免加锁，提高执行效率。
2. 更新一个存在的擦除对象时，需要加锁将对象设置为nil，添加到dirty中，再从nil更新为新值。
3. 更新一个存在于dirty中的对象时，加锁将它的值设置为new\_value
4. 更新一个新对象时，加锁更新read的状态amended为true，表示dirty含有read不存在的对象，并将新key对应的对象存入dirty

Note: 在存储对象时，虽然某些情况下存在加锁行为，但并未累计加锁数量 misses.

```
func (m *Map) Store(key, value interface{}) {
     // 1. 更新已存在的非擦除态对象
     // 使用CAS原子操作，避免加锁
     read, _ := m.read.Load().(readOnly)
     if e, ok := read.m[key]; ok && e.tryStore(&value) {
        return
     }

     // 2. 加锁
     m.mu.Lock()
     read, _ = m.read.Load().(readOnly)
     if e, ok := read.m[key]; ok {
         // 2.1.1 更新已存在的擦除态对象，先更新它的状态为nil，
         // 并将它添加到dirty中
         if e.unexpungeLocked() {
            // The entry was previously expunged, which implies that there is a
            // non-nil dirty map and this entry is not in it.
            m.dirty[key] = e
         }
         // 2.1.2 将nil更新为为new_value
         e.storeLocked(&value)
    } else if e, ok := m.dirty[key]; ok {
         // 2.2 更新存在与dirty中对象，将它的值更新为new_value
         e.storeLocked(&value)
    } else {
         // 2.3.1 添加一个新对象，先更新read.amended，表示dirty中存在
         // read不含有得key
         if !read.amended {
              m.dirtyLocked()
              m.read.Store(readOnly{m: read.m, amended: true})
         }
         // 2.3.2 将key对应的值存入dirty
         m.dirty[key] = newEntry(value)
     }
     m.mu.Unlock()
}

// 将entry中元素的状态由expunged修改为nil
func (e *entry) unexpungeLocked() (wasExpunged bool) {
     return atomic.CompareAndSwapPointer(&e.p, expunged, nil)
}

func (m *Map) dirtyLocked() {
     if m.dirty != nil {
        return
     }

      // 当dirty提升为read后，dirty会被置为nil
      // 因此当未来向dirty中添加元素时，需要先构建新dirty的状态
      // 因为dirty要包含所有有效的key元素，所以新构建dirty状态时，
      // 将read中存储有效值的数据也存储至dirty中
      read, _ := m.read.Load().(readOnly)
      m.dirty = make(map[interface{}]*entry, len(read.m))
      for k, e := range read.m {
           // 将包含有效值的元素存入dirty
           if !e.tryExpungeLocked() {
               m.dirty[k] = e
           }
      }
}

// 将entry中状态为nil的元素置为expunged，表示它已被逻辑删除
// 当返回值为true时，表示元素已被删除，否则，元素为一个有效值
func (e *entry) tryExpungeLocked() (isExpunged bool) {
      p := atomic.LoadPointer(&e.p)
      for p == nil {
          if atomic.CompareAndSwapPointer(&e.p, nil, expunged) {
              return true
          }
          p = atomic.LoadPointer(&e.p)
      }
      return p == expunged
}
```

### Delete 删除操作

删除Key对应的元素，对于不同的对象，有不同的删除方式

1. 存在于read中的对象，使用原子操作避免加锁，将查询到的对象使用CAS操作将元素值置为nil，从逻辑上删除
2. 仅存在于dirty的对象，累计命中率丢失的次数，并使用CAS操作将元素值置为nil，从逻辑上删除
```
// Delete deletes the value for a key.
func (m *Map) Delete(key interface{}) {
     m.LoadAndDelete(key)
}

func (m *Map) LoadAndDelete(key interface{}) (value interface{}, loaded bool) {
      //1. 使用原子操作查询read状态
      read, _ := m.read.Load().(readOnly)
      e, ok := read.m[key]
      if !ok && read.amended {
           //2. 未查到，加锁查询dirty
           m.mu.Lock()
           read, _ = m.read.Load().(readOnly)
           e, ok = read.m[key]
           if !ok && read.amended {
                e, ok = m.dirty[key]
                delete(m.dirty, key)
                //3. 累计加锁的数量，触发状态提升
                m.missLocked()
           }
           m.mu.Unlock()
      }
      // 4. 查询到key，将key的值置为nil，进行逻辑上删除
     if ok {
          return e.delete()
     }
     return nil, false
}

// 将元素从逻辑上进行删除，将状态置为nil
func (e *entry) delete() (value interface{}, ok bool) {
     for {
         p := atomic.LoadPointer(&e.p)
         if p == nil || p == expunged {
              return nil, false
         }
         if atomic.CompareAndSwapPointer(&e.p, p, nil) {
              return *(*interface{})(p), true
         }
      }
}
```

### Range 遍历操作

提供了O(N)的方式遍历sync.Map，用户传入遍历key-value的动作函数，如果函数返回false，则遍历终止；即使在遍历过程中发生key的并发读写操作，每个key也仅会被最多遍历一次。 因为 dirty可能包含read中不存在的key的状态，当遍历操作发生时，如果dirty与read存储的有效key的状态不一致，将dirty提升为read。

1. read包含所有有效元素时，直接便利read中存储的值
2. dirty含有read中不存在的元素时，将dirty提升为read，再遍历新read中存储的值
```
func (m *Map) Range(f func(key, value interface{}) bool) {
      read, _ := m.read.Load().(readOnly)
      // 1. dirty中包含read不存在的状态，
      // 将dirty提升为read.
      if read.amended {
          m.mu.Lock()
          read, _ = m.read.Load().(readOnly)
          if read.amended {
              read = readOnly{m: m.dirty}
              m.read.Store(read)
              m.dirty = nil
              m.misses = 0
          }
          m.mu.Unlock()
       }

       // 2. 遍历read，当动作函数返回false时，退出遍历。
       // 由于遍历过程中可能存在map的并发修改操作，因此当遍历该entry时，
       // 实际存储的值被删除时，则不再遍历.
       for k, e := range read.m {
           v, ok := e.load()
           if !ok {
               continue
           }
           if !f(k, v) {
                 break
           }
       }   
}

func (e *entry) load() (value interface{}, ok bool) {
      p := atomic.LoadPointer(&e.p)
      if p == nil || p == expunged {
          return nil, false
      }
      return *(*interface{})(p), true
}
```

### LoadAndDelete 复合操作

与Delete操作一样，仅增加了返回值传递被删除的对象.

### LoadOrStore 复合操作

如果key存在，则返回已存在的值，否则将传入的参数存入Map并返回。loaded 为true ，表示返回的值为以前存在的旧值，值为false，返回的值为传入的参数。

执行逻辑与Store类似，也是4种场景

1. read中存在key的有效值，不更新返回已存在的值
2. read中存在key但是它的值被逻辑删除nil，则将其更新为传入的新值；
3. read中存在key但是它的值被擦除expunged，则先将它的状态更新为nil，将它存入dirty，并更新它为传入的新值
4. dirty中存在key的有效值，不更新且返回已存在的值
5. dirty中存在key但是它的值被逻辑删除nil，则将其更新为传入的新值
6. 一个新加入的key，将它存入dirty，并更新readOnly的amended状态未true
```
func (m *Map) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
      // Avoid locking if it's a clean hit.
      read, _ := m.read.Load().(readOnly)
      if e, ok := read.m[key]; ok {
            actual, loaded, ok := e.tryLoadOrStore(value)
            if ok {
                return actual, loaded
            }
      }

      m.mu.Lock()
      read, _ = m.read.Load().(readOnly)
      if e, ok := read.m[key]; ok {
          if e.unexpungeLocked() {
              m.dirty[key] = e
          }
          actual, loaded, _ = e.tryLoadOrStore(value)
      } else if e, ok := m.dirty[key]; ok {
            actual, loaded, _ = e.tryLoadOrStore(value)
            m.missLocked()
      } else {
            if !read.amended {
                // We're adding the first new key to the dirty map.
                // Make sure it is allocated and mark the read-only map as incomplete.
                m.dirtyLocked()
                m.read.Store(readOnly{m: read.m, amended: true})
             }
             m.dirty[key] = newEntry(value)
             actual, loaded = value, false
      }
      m.mu.Unlock()

      return actual, loaded
}
```

tryLoadOrStore执行逻辑，entry中存储的值存在三种状态：有效值、nil、expunged

1. expunged：该值已被擦除，当前仅存在于read状态中，不允许直接更新为新值，因此直接返回。
2. 有效值：存在一个有效值，直接返回该旧值，不更新为新值。
3. nil: key对应的对象被逻辑删除，可以被设置为新值，如果设置成功则返回。因为entry会被其它go程并发读写调用，因此更新失败时需要判断它的状态是否为expunged或有效值状态，是则表示值被其它go程更新，返回对应的值。
```
func (e *entry) tryLoadOrStore(i interface{}) (actual interface{}, loaded, ok bool) {
      p := atomic.LoadPointer(&e.p)
      if p == expunged {
           return nil, false, false
      }
      if p != nil {
           return *(*interface{})(p), true, true
      }

      ic := i
      for {
            if atomic.CompareAndSwapPointer(&e.p, nil, unsafe.Pointer(&ic)) {
                  return i, false, true
            }
            p = atomic.LoadPointer(&e.p)
            if p == expunged {
                 return nil, false, false
            }
            if p != nil {
                   return *(*interface{})(p), true, true
            }
      }
}
```

## 总结

**1\. Map适用的场景：**

1. 读多写少的场景，具体到key的细节场景为下列两种情况
1. key被一写多读的场景，因为当key存在与read中时，利用原子操作来避免上锁来提升效率；同时通过及时将dirty提升为read，减少查询读状态时的miss次数
		2. 并发更新已存在的不同key的场景，利用原子的CAS操作更新已存在的值

**2\. Map不适用的场景：**

1. 读写相等或写多读少的场景，原因
1. 因为新增的key初始仅存在于dirty中，此时的存储操作需要加锁
		2. 读写相等的情况下（如key写一次、读一次），读操作时无法从read状态命中，从而需要加锁读取dirty状态，相比于简单的Mutex增加了额外的执行逻辑；写多读少也是类似，dirty的元素增加速率大于read的命中率缺失，从而迟迟无法触发状态提升(dirty->read)，会导致读取操作极容易触发加锁读取dirty的过程
	3. 大量读取不存在的key的场景，原因
1. 频繁触发状态提升 dirty->read，因为key不存在导致read的丢失命中率极容易达到阈值
		2. Load相比于普通的Mutex加锁处理，多执行很多逻辑

**3\. Map的设计原理**

1. 读写分离，使用原子操作提升并发场景下的读操作性能
	2. 采用逻辑删除，批量删除已被擦除的key，将实际的删除成本摊销
	3. 累计只读read状态的命中率缺失次数，及时将dirty提升为read，提高命中率
	4. 读写两个状态中存储对象的指针，read中数据被CAS修改后的值对两个状态都可见

**欢迎点赞分享，搜索关注【鹅厂架构师】公众号，一起探索更多业界领先产品技术。**

发布于 2023-07-06 10:30・广东