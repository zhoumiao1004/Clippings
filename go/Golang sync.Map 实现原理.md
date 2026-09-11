## 1 前言

golang 中，map 不是并发安全的结构，并发读写会引发严重的错误.

sync 标准包下的 sync.Map 能解决 map 并发读写的问题，本文通过手撕源码+梳理流程的方式，和大家一起讨论其底层实现原理，并进一步总结出 sync.Map 的特征和适用场景.

## 2 核心数据结构

## 2.1 sync.Map

![](https://pic2.zhimg.com/v2-b639f56e56b355507d16932e63f55e19_1440w.jpg)

```
type Map struct {
    mu Mutex
    read atomic.Value 
    dirty map[any]*entry
    misses int
}
```

sync.Map 主类中包含以下核心字段：

（1）read：无锁化的只读 map，实际类型为 [readOnly](https://zhida.zhihu.com/search?content_id=221283434&content_type=Article&match_order=1&q=readOnly&zhida_source=entity) ，2.3 小节会进一步介绍；

（2）dirty：加锁处理的读写 map；

（3）misses：记录访问 read 的失效次数，累计达到阈值时，会进行 read map/dirty map 的更新轮换；

（4）mu：一把互斥锁，实现 dirty 和 misses 的并发管理.

可见，sync.Map 的特点是冗余了两份 map：read map 和 dirty map，后续的所介绍的交互流程也和这两个 map 息息相关，基本可以归结为两条主线：

主线一：首先基于无锁操作访问 read map；倘若 read map 不存在该 key，则加锁并使用 dirty map 兜底；

主线二：read map 和 dirty map 之间会交替轮换更新.

## 2.2 entry 及对应的几种状态

```
type entry struct {
    p unsafe.Pointer 
}
```

kv 对中的 value，统一采用 unsafe.Pointer 的形式进行存储，通过 entry.p 的指针进行链接.

entry.p 的指向分为三种情况：

I 存活态：正常指向元素；

II 软删除态：指向 nil；

III 硬删除态：指向固定的全局变量 expunged.

```
var expunged = unsafe.Pointer(new(any))
```

（1）存活态很好理解，即 key-entry 对仍未删除；

（2）nil 态表示软删除，read map 和 dirty map 底层的 map 结构仍存在 key-entry 对，但在逻辑上该 key-entry 对已经被删除，因此无法被用户查询到；

（3）expunged 态表示硬删除，dirty map 中已不存在该 key-entry 对.

## 2.3 readOnly

```
type readOnly struct {
    m       map[any]*entry
    amended bool // true if the dirty map contains some key not in m.
}
```

sync.Map 中的只读 map：read 内部包含两个成员属性：

（1）m：真正意义上的 read map，实现从 key 到 entry 的映射；

（2）amended：标识 read map 中的 key-entry 对是否存在缺失，需要通过 dirty map 兜底.

## 3 读流程

![](https://pica.zhimg.com/v2-97c7190266336d9b243bb289c54a8076_1440w.jpg)

## 3.1 sync.Map.Load()

```
func (m *Map) Load(key any) (value any, ok bool) {
    read, _ := m.read.Load().(readOnly)
    e, ok := read.m[key]
    if !ok && read.amended {
        m.mu.Lock()
        read, _ = m.read.Load().(readOnly)
        e, ok = read.m[key]
        if !ok && read.amended {
            e, ok = m.dirty[key]
            m.missLocked()
        }
        m.mu.Unlock()
    }
    if !ok {
        return nil, false
    }
    return e.load()
}
```

（1）查看 read map 中是否存在 key-entry 对，若存在，则直接读取 entry 返回；

（2）倘若第一轮 read map 查询 miss，且 read map 不全，则需要加锁 double check；

（3）第二轮 read map 查询仍 miss（加锁后），且 read map 不全，则查询 dirty map 兜底；

（4）查询操作涉及到与 dirty map 的交互，misses 加一；

（5）解锁，返回查得的结果.

## 3.2 entry.load()

```
func (e *entry) load() (value any, ok bool) {
    p := atomic.LoadPointer(&e.p)
    if p == nil || p == expunged {
        return nil, false
    }
    return *(*any)(p), true
}
```

（1）sync.Map 中，kv 对的 value 是基于 entry 指针封装的形式；

（2）从 map 取得 entry 后，最终需要调用 entry.load 方法读取指针指向的内容；

（3）倘若 entry 的指针状态为 nil 或者 expunged，说明 key-entry 对已被删除，则返回 nil；

（4）倘若 entry 未被删除，则读取指针内容，并且转为 any 的形式进行返回.

## 3.3 sync.Map.missLocked()

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

（1）在读流程中，倘若未命中 read map，且由于 read map 内容存在缺失需要和 dirty map 交互时，会走进 missLocked 流程；

（2）在 missLocked 流程中，首先 misses 计数器累加 1；

（3）倘若 miss 次数小于 dirty map 中存在的 key-entry 对数量，直接返回即可；

（4）倘若 miss 次数大于等于 dirty map 中存在的 key-entry 对数量，则使用 dirty map 覆盖 read map，并将 read map 的 amended flag 置为 false；

（5）新的 dirty map 置为 nil，misses 计数器清零.

## 4 写流程

![](https://pic2.zhimg.com/v2-84417f03a84c6581c645eeb2209d2253_1440w.jpg)

## 4.1 sync.Map.Store()

```
func (m *Map) Store(key, value any) {
    read, _ := m.read.Load().(readOnly)
    if e, ok := read.m[key]; ok && e.tryStore(&value) {
        return
    }

    m.mu.Lock()
    read, _ = m.read.Load().(readOnly)
    if e, ok := read.m[key]; ok {
        if e.unexpungeLocked() {
            m.dirty[key] = e
        }
        e.storeLocked(&value)
    } else if e, ok := m.dirty[key]; ok {
        e.storeLocked(&value)
    } else {
        if !read.amended {
            m.dirtyLocked()
            m.read.Store(readOnly{m: read.m, amended: true})
        }
        m.dirty[key] = newEntry(value)
    }
    m.mu.Unlock()
}

func (e *entry) storeLocked(i *any) {
    atomic.StorePointer(&e.p, unsafe.Pointe
}
```

（1）倘若 read map 存在拟写入的 key，且 entry 不为 expunged 状态，说明这次操作属于更新而非插入，直接基于 [CAS](https://zhida.zhihu.com/search?content_id=221283434&content_type=Article&match_order=1&q=CAS&zhida_source=entity) 操作进行 entry 值的更新，并直接返回（存活态或者软删除，直接覆盖更新）；

（2）倘若未命中（1）的分支，则需要加锁 double check；

（3）倘若第二轮检查中发现 read map 或者 dirty map 中存在 key-entry 对，则直接将 entry 更新为新值即可（存活态或者软删除，直接覆盖更新）；

（4）在第（3）步中，如果发现 read map 中该 key-entry 为 expunged 态，需要在 dirty map 先补齐 key-entry 对，再更新 entry 值（从硬删除中恢复，然后覆盖更新）；

（5）倘若 read map 和 dirty map 均不存在，则在 dirty map 中插入新 key-entry 对，并且保证 read map 的 amended flag 为 true.（插入）

（6）第（5）步的分支中，倘若发现 dirty map 未初始化，需要前置执行 dirtyLocked 流程；

（7）解锁返回.

下面补充介绍 Store() 方法中涉及到的几个子方法.

## 4.2 entry.tryStore()

```
func (m *Map) Store(key, value any) {
    read, _ := m.read.Load().(readOnly)
    if e, ok := read.m[key]; ok && e.tryStore(&value) {
        return
    }

    m.mu.Lock()
   // ...
}

func (e *entry) tryStore(i *any) bool {
    for {
        p := atomic.LoadPointer(&e.p)
        if p == expunged {
            return false
        }
        if atomic.CompareAndSwapPointer(&e.p, p, unsafe.Pointer(i)) {
            return true
        }
    }
}
```

（1）在写流程中，倘若发现 read map 中已存在对应的 key-entry 对，则会对调用 tryStore 方法尝试进行更新；

（2）倘若 entry 为 expunged 态，说明已被硬删除，dirty 中缺失该项数据，因此 tryStore 执行失败，回归主干流程；

（3）倘若 entry 非 expunged 态，则直接执行 CAS 操作完成值的更新即可.

## 4.3 entry.unexpungeLocked()

```
func (m *Map) Store(key, value any) {
    // ...
    m.mu.Lock()
    read, _ = m.read.Load().(readOnly)
    if e, ok := read.m[key]; ok {
        if e.unexpungeLocked() {
            m.dirty[key] = e
        }
        e.storeLocked(&value)
    } 
    // ...
}

func (e *entry) unexpungeLocked() (wasExpunged bool) {
    return atomic.CompareAndSwapPointer(&e.p, expunged, nil)
}
```

（1）在写流程加锁 double check 的过程中，倘若发现 read map 中存在对应的 key-entry 对，会执行该方法；

（2）倘若 key-entry 为硬删除 expunged 态，该方法会基于 CAS 操作将其更新为软删除 nil 态，然后进一步在 dirty map 中补齐该 key-entry 对，实现从硬删除到软删除的恢复.

## 4.4 entry.storeLocked()

```
func (m *Map) Store(key, value any) {
    // ...
    m.mu.Lock()
    read, _ = m.read.Load().(readOnly)
    if e, ok := read.m[key]; ok {
       // ...
        e.storeLocked(&value)
    } else if e, ok := m.dirty[key]; ok {
        e.storeLocked(&value)
    } 
    // ...
}

func (e *entry) storeLocked(i *any) {
    atomic.StorePointer(&e.p, unsafe.Pointer)
}
```

写流程中，倘若 read map 或者 dirty map 存在对应 key-entry，最终会通过 [原子操作](https://zhida.zhihu.com/search?content_id=221283434&content_type=Article&match_order=1&q=%E5%8E%9F%E5%AD%90%E6%93%8D%E4%BD%9C&zhida_source=entity) ，将新值的指针存储到 entry.p 当中.

## 4.5 sync.Map.dirtyLocked()

![](https://pic4.zhimg.com/v2-d79acb1304ee6be6af0ac8c8d0875279_1440w.jpg)

```
func (m *Map) dirtyLocked() {
    if m.dirty != nil {
        return
    }

    read, _ := m.read.Load().(readOnly)
    m.dirty = make(map[any]*entry, len(read.m))
    for k, e := range read.m {
        if !e.tryExpungeLocked() {
            m.dirty[k] = e
        }
    }
}

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

（1）在写流程中，倘若需要将 key-entry 插入到兜底的 dirty map 中，并且此时 dirty map 为空（从未写入过数据或者刚发生过 missLocked），会进入 dirtyLocked 流程；

（2）此时会遍历一轮 read map ，将未删除的 key-entry 对拷贝到 dirty map 当中；

（3）在遍历时，还会将 read map 中软删除 nil 态的 entry 更新为硬删除 expunged 态，因为在此流程中，不会将其拷贝到 dirty map.

## 5 删流程

![](https://pic4.zhimg.com/v2-821da17d6bd0a16e302c45d0917cb30d_1440w.jpg)

## 5.1 sync.Map.Delete()

```
func (m *Map) Delete(key any) {
    m.LoadAndDelete(key)
}

func (m *Map) LoadAndDelete(key any) (value any, loaded bool) {
    read, _ := m.read.Load().(readOnly)
    e, ok := read.m[key]
    if !ok && read.amended {
        m.mu.Lock()
        read, _ = m.read.Load().(readOnly)
        e, ok = read.m[key]
        if !ok && read.amended {
            e, ok = m.dirty[key]
            delete(m.dirty, key)
            m.missLocked()
        }
        m.mu.Unlock()
    }
    if ok {
        return e.delete()
    }
    return nil, false
}
```

（1）倘若 read map 中存在 key，则直接基于 cas 操作将其删除；

（2）倘若read map 不存在 key，且 read map 有缺失（amended flag 为 true），则加锁 dou check；

（3）倘若加锁 double check 时，read map 仍不存在 key 且 read map 有缺失，则从 dirty map 中取元素，并且将 key-entry 对从 dirty map 中物理删除；

（4）走入步骤（3），删操作需要和 dirty map 交互，需要走进 3.3 小节介绍的 missLocked 流程；

（5）解锁；

（6）倘若从 read map 或 dirty map 中获取到了 key 对应的 entry，则走入 entry.delete() 方法逻辑删除 entry；

（7）倘若 read map 和 dirty map 中均不存在 key，返回 false 标识删除失败.

## 5.2 entry.delete()

```
func (e *entry) delete() (value any, ok bool) {
    for {
        p := atomic.LoadPointer(&e.p)
        if p == nil || p == expunged {
            return nil, false
        }
        if atomic.CompareAndSwapPointer(&e.p, p, nil) {
            return *(*any)(p), true
        }
    }
}
```

（1）该方法是 entry 的逻辑删除方法；

（2）倘若 entry 此前已被删除，则直接返回 false 标识删除失败；

（3）倘若 entry 当前仍存在，则通过 CAS 将 entry.p 指向 nil，标识其已进入软删除状态.

## 6 遍历流程

![](https://pica.zhimg.com/v2-70ebd7e293a3628e86ec04c834979768_1440w.jpg)

```
func (m *Map) Range(f func(key, value any) bool) {
    read, _ := m.read.Load().(readOnly)
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
```

（1）在遍历过程中，倘若发现 read map 数据不全（amended flag 为 true），会额外加一次锁，并使用 dirty map 覆盖 read map；

（2）遍历 read map（通过步骤（1）保证 read map 有全量数据），执行用户传入的回调函数，倘若某次回调时返回值为 false，则会终止全流程.

## 7 总结

## 7.1 entry 的 expunged 态

**思考问题：**

为什么需要使用 expunged 态来区分软硬删除呢？仅用 nil 一种状态来标识删除不可以吗？

**回答：**

首先需要明确，无论是 [软删除](https://zhida.zhihu.com/search?content_id=221283434&content_type=Article&match_order=9&q=%E8%BD%AF%E5%88%A0%E9%99%A4&zhida_source=entity) (nil)还是硬删除(expunged),都表示在逻辑意义上 key-entry 对已经从 sync.Map 中删除，nil 和 expunged 的区别在于：

（1）软删除态（nil）：read map 和 dirty map 在物理上仍保有该 key-entry 对，因此倘若此时需要对该 entry 执行写操作，可以直接 CAS 操作；

（2）硬删除态（expunged）： dirty map 中已经没有该 key-entry 对，倘若执行写操作，必须加锁（dirty map 必须含有全量 key-entry 对数据）.

![](https://pica.zhimg.com/v2-1f09f2583c5f611adabf10f9743e6f80_1440w.jpg)

设计 expunged 和 nil 两种状态的原因，就是为了优化在 dirtyLocked 前，针对同一个 key 先删后写的场景. 通过 expunged 态额外标识出 dirty map 中是否仍具有指向该 entry 的能力，这样能够实现对一部分 nil 态 key-entry 对的解放，能够基于 CAS 完成这部分内容写入操作而无需加锁.

## 7.2 read map 和 dirty map 的数据流转

sync.Map 由两个 map 构成：

（1）read map：访问时全程无锁；

（2）dirty map：是兜底的读写 map，访问时需要加锁.

之所以这样处理，是希望能根据对读、删、更新、写操作频次的探测，来实时动态地调整操作方式，希望在读、更新、删频次较高时，更多地采用 CAS 的方式无锁化地完成操作；在写操作频次较高时，则直接了当地采用加锁操作完成.

因此， sync.Map 本质上采取了一种以空间换时间 + 动态调整策略的设计思路，下面对两个 map 间的数据流转过程进行详细介绍：

### 7.2.1 两个 map

（1）总体思想，希望能多用 read map，少用 dirty map，因为操作前者无锁，后者需要加锁；

（2）除了 expunged 态的 entry 之外，read map 的内容为 dirty map 的子集；

### 7.2.2 dirty map -> read map

![](https://pica.zhimg.com/v2-2a5ab86b61c34f0fce3d05341cd0be1e_1440w.jpg)

（3）记录读/删流程中，通过 misses 记录访问 read map miss 由 dirty 兜底处理的次数，当 miss 次数达到阈值，则进入 missLocked 流程，进行新老 read/dirty 替换流程；此时将老 dirty 作为新 read，新 dirty map 则暂时为空，直到 dirtyLocked 流程完成对 dirty 的初始化；

### 7.2.3 read map -> dirty map

![](https://picx.zhimg.com/v2-6ad2a1b762d8f540a8ccc504e1600d9d_1440w.jpg)

（4）发生 dirtyLocked 的前置条件： I dirty 暂时为空（此前没有写操作或者近期进行过 missLocked 流程）；II 接下来一次写操作访问 read 时 miss，需要由 dirty 兜底；

（5）在 dirtyLocked 流程中，需要对 read 内的元素进行状态更新，因此需要遍历，是一个线性时间复杂度的过程，可能存在性能抖动；

（6）dirtyLocked 遍历中，会将 read 中未被删除的元素（非 nil 非 expunged）拷贝到 dirty 中；会将 read 中所有此前被删的元素统一置为 expunged 态.

## 7.3 适用场景与注意问题

综合全文，做个总结：

（1）sync.Map 适用于读多、更新多、删多、写少的场景；

（2）倘若写操作过多，sync.Map 基本等价于互斥锁 + map；

（3）sync.Map 可能存在性能抖动问题，主要发生于在读/删流程 miss 只读 map 次数过多时（触发 missLocked 流程），下一次插入操作的过程当中（dirtyLocked 流程）.

文末小广告：

欢迎老板们关注我的个人公众号：小徐先生的编程世界

我会不定期更新个人纯原创的编程技术博客，技术栈以 go 语言为主，让我们一起点亮更多的编程技能树吧！

发布于 2023-01-15 11:32・北京