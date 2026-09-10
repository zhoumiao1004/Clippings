
用过 go 的都知道 [channel](https://zhida.zhihu.com/search?content_id=220851325&content_type=Article&match_order=1&q=channel&zhida_source=entity) ，无需多言，直接开整！
## 1 核心数据结构

![](https://pic3.zhimg.com/v2-33e6558c73c76c439818cb4477bfb14a_1440w.jpg)

### 1.1 hchan
```
type hchan struct {
    qcount   uint           // total data in the queue
    dataqsiz uint           // size of the circular queue
    buf      unsafe.Pointer // points to an array of dataqsiz elements
    elemsize uint16
    closed   uint32
    elemtype *_type // element type
    sendx    uint   // send index
    recvx    uint   // receive index
    recvq    waitq  // list of recv waiters
    sendq    waitq  // list of send waiters

    lock mutex
}
```
hchan： channel 数据结构
（1）qcount：当前 channel 中存在多少个元素；
（2）dataqsize: 当前 channel 能存放的元素容量；
（3）buf：channel 中用于存放元素的环形缓冲区；
（4）elemsize：channel 元素类型的大小；
（5）closed：标识 channel 是否关闭；
（6）elemtype：channel 元素类型；
（7）sendx：发送元素进入 [环形缓冲区](https://zhida.zhihu.com/search?content_id=220851325&content_type=Article&match_order=2&q=%E7%8E%AF%E5%BD%A2%E7%BC%93%E5%86%B2%E5%8C%BA&zhida_source=entity) 的 index；
（8）recvx：接收元素所处的环形缓冲区的 index；
（9）recvq：因接收而陷入阻塞的协程队列；
（10）sendq：因发送而陷入阻塞的协程队列；
### 1.2 waitq

```
type waitq struct {
    first *sudog
    last  *sudog
}
```

waitq：阻塞的协程队列

（1）first：队列头部

（2）last：队列尾部

### 1.3 sudog

```go
type sudog struct {
    g *g

    next *sudog
    prev *sudog
    elem unsafe.Pointer // data element (may point to stack)
    // ...
    c        *hchan 
}
```
sudog：用于包装协程的节点
（1）g：goroutine，协程；
（2）next：队列中的下一个节点；
（3）prev：队列中的前一个节点；
（4）elem: 读取/写入 channel 的数据的容器;
（5）c：标识与当前 sudog 交互的 chan.
## 2 构造器函数

![](https://pic3.zhimg.com/v2-112b94a7ac698ee34d0b4a7c5641f388_1440w.jpg)

```go
func makechan(t *chantype, size int) *hchan {
    elem := t.elem

    // ...
    mem, overflow := math.MulUintptr(elem.size, uintptr(size))
    if overflow || mem > maxAlloc-hchanSize || size < 0 {
        panic(plainError("makechan: size out of range"))
    }

    var c *hchan
    switch {
    case mem == 0:
        // Queue or element size is zero.
        c = (*hchan)(mallocgc(hchanSize, nil, true))
    case elem.ptrdata == 0:
        // Elements do not contain pointers.
        // Allocate hchan and buf in one call.
        c = (*hchan)(mallocgc(hchanSize+mem, nil, true))
        c.buf = add(unsafe.Pointer(c), hchanSize)
    default:
        // Elements contain pointers.
        c = new(hchan)
        c.buf = mallocgc(mem, elem, true)
    }

    c.elemsize = uint16(elem.size)
    c.elemtype = elem
    c.dataqsiz = uint(size)
    
    lockInit(&c.lock, lockRankHchan)

    return
}
```
（1）判断申请内存空间大小是否越界，mem 大小为 element 类型大小与 element 个数相乘后得到，仅当无缓冲型 channel 时，因个数为 0 导致大小为 0；
（2）根据类型，初始 channel，分为 无缓冲型、有缓冲元素为 struct 型、有缓冲元素为 pointer 型 channel;
（3）倘若为无缓冲型，则仅申请一个大小为默认值 96 的空间；
（4）如若有缓冲的 struct 型，则一次性分配好 96 + mem 大小的空间，并且调整 chan 的 buf 指向 mem 的起始位置；
（5）倘若为有缓冲的 pointer 型，则分别申请 chan 和 buf 的空间，两者无需连续；
（6）对 channel 的其余字段进行初始化，包括元素类型大小、元素类型、容量以及锁的初始化.

## 3 写流程

### 3.1 两类异常情况处理
```go
func chansend1(c *hchan, elem unsafe.Pointer) {
    chansend(c, elem, true, getcallerpc())
}

func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
    if c == nil {
        gopark(nil, nil, waitReasonChanSendNilChan, traceEvGoStop, 2)
        throw("unreachable")
    }

    lock(&c.lock)

    if c.closed != 0 {
        unlock(&c.lock)
        panic(plainError("send on closed channel"))
    }
}
```
（1）对于未初始化的 chan，写入操作会引发死锁；
（2）对于已关闭的 chan，写入操作会引发 panic.
### 3.2 case1：写时存在阻塞读协程

![](https://pica.zhimg.com/v2-aeebdcfb97a933e7ab5a6629a9ec385c_1440w.jpg)

```go
func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
    // ...
    lock(&c.lock)
    // ...

    if sg := c.recvq.dequeue(); sg != nil {
        // Found a waiting receiver. We pass the value we want to send
        // directly to the receiver, bypassing the channel buffer (if any).
        send(c, sg, ep, func() { unlock(&c.lock) }, 3)
        return true
    }
    // ..
}
```
（1）加锁；
（2）从阻塞度协程队列中取出一个 goroutine 的封装对象 sudog；
（3）在 send 方法中，会基于 memmove 方法，直接将元素拷贝交给 sudog 对应的 goroutine；
（4）在 send 方法中会完成解锁动作.

### 3.3 case2：写时无阻塞读协程但环形缓冲区仍有空间

```
func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
    // ...
    lock(&c.lock)
    // ...
    if c.qcount < c.dataqsiz {
        // Space is available in the channel buffer. Enqueue the element to send.
        qp := chanbuf(c, c.sendx)
        typedmemmove(c.elemtype, qp, ep)
        c.sendx++
        if c.sendx == c.dataqsiz {
            c.sendx = 0
        }
        c.qcount++
        unlock(&c.lock)
        return true
    }

    // ...
}
```
（1）加锁；
（2）将当前元素添加到环形缓冲区 sendx 对应的位置；
（3）sendx++;
（4）qcount++;
（4）解锁，返回.
### 3.4 case3：写时无阻塞读协程且环形缓冲区无空间

![](https://pic2.zhimg.com/v2-0add82424b36cbd0a5edf54d115cf4db_1440w.jpg)

```go
func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
    // ...
    lock(&c.lock)

    // ...
    gp := getg()
    mysg := acquireSudog()
    mysg.elem = ep
    mysg.g = gp
    mysg.c = c
    gp.waiting = mysg
    c.sendq.enqueue(mysg)
    
    atomic.Store8(&gp.parkingOnChan, 1)
    gopark(chanparkcommit, unsafe.Pointer(&c.lock), waitReasonChanSend, traceEvGoBlockSend, 2)
    
    gp.waiting = nil
    closed := !mysg.success
    gp.param = nil
    mysg.c = nil
    releaseSudog(mysg)
    return true
}
```

（1）加锁；

（2）构造封装当前 goroutine 的 sudog 对象；

（3）完成指针指向，建立 sudog、goroutine、channel 之间的指向关系；

（4）把 sudog 添加到当前 channel 的阻塞写协程队列中；

（5）park 当前协程；

（6）倘若协程从 park 中被唤醒，则回收 sudog（sudog能被唤醒，其对应的元素必然已经被读协程取走）；

（7）解锁，返回

### 3.5 写流程整体串联

![](https://pic2.zhimg.com/v2-152528930008eb3508fcb0b5ffe480a3_1440w.jpg)

## 4 读流程

### 4.1 异常 case1：读空 channel

```
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {
    if c == nil {
        gopark(nil, nil, waitReasonChanReceiveNilChan, traceEvGoStop, 2)
        throw("unreachable")
    }
    // ...
}
```

（1）park 挂起，引起死锁；

### 4.2 异常 case2：channel 已关闭且内部无元素

```
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {
  
    lock(&c.lock)

    if c.closed != 0 {
        if c.qcount == 0 {
            unlock(&c.lock)
            if ep != nil {
                typedmemclr(c.elemtype, ep)
            }
            return true, false
        }
        // The channel has been closed, but the channel's buffer have data.
    } 

    // ...
}
```

（1）直接解锁返回即可

### 4.3 case3：读时有阻塞的写协程

![](https://picx.zhimg.com/v2-867428e66aeded401bb24ef760d99269_1440w.jpg)

```
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {
   
    lock(&c.lock)

    // Just found waiting sender with not closed.
    if sg := c.sendq.dequeue(); sg != nil {
        recv(c, sg, ep, func() { unlock(&c.lock) }, 3)
        return true, true
     }
}

func recv(c *hchan, sg *sudog, ep unsafe.Pointer, unlockf func(), skip int) {
    if c.dataqsiz == 0 {
        if ep != nil {
            // copy data from sender
            recvDirect(c.elemtype, sg, ep)
        }
    } else {
        // Queue is full. Take the item at the
        // head of the queue. Make the sender enqueue
        // its item at the tail of the queue. Since the
        // queue is full, those are both the same slot.
        qp := chanbuf(c, c.recvx)
        if ep != nil {
            typedmemmove(c.elemtype, ep, qp)
        }
        
        typedmemmove(c.elemtype, qp, sg.elem)
        c.recvx++
        if c.recvx == c.dataqsiz {
            c.recvx = 0
        }
        c.sendx = c.recvx // c.sendx = (c.sendx+1) % c.dataqsiz
    }
    sg.elem = nil
    gp := sg.g
    unlockf()
    gp.param = unsafe.Pointer(sg)
    sg.success = true
    goready(gp, skip+1)
}
```

（1）加锁；

（2）从阻塞写协程队列中获取到一个写协程；

（3）倘若 channel 无缓冲区，则直接读取写协程元素，并唤醒写协程；

（4）倘若 channel 有缓冲区，则读取缓冲区头部元素，并将写协程元素写入缓冲区尾部后唤醒写写成；

（5）解锁，返回.

### 4.4 case4：读时无阻塞写协程且缓冲区有元素

![](https://pic1.zhimg.com/v2-354eb67a148726103d12d08b9d5c3b04_1440w.jpg)

```
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {

    lock(&c.lock)

    if c.qcount > 0 {
        // Receive directly from queue
        qp := chanbuf(c, c.recvx)
        if ep != nil {
            typedmemmove(c.elemtype, ep, qp)
        }
        typedmemclr(c.elemtype, qp)
        c.recvx++
        if c.recvx == c.dataqsiz {
            c.recvx = 0
        }
        c.qcount--
        unlock(&c.lock)
        return true, true
    }
}
```

（1）加锁；

（2）获取到 recvx 对应位置的元素；

（3）recvx++

（4）qcount--

（5）解锁，返回

### 4.5 case5：读时无阻塞写协程且缓冲区无元素

![](https://pic3.zhimg.com/v2-b3070eedf5f179b7a7515dc0ec5892d0_1440w.jpg)

```
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {
    lock(&c.lock)

    gp := getg()
    mysg := acquireSudog()
    mysg.elem = ep
    gp.waiting = mysg
    mysg.g = gp
    mysg.c = c
    gp.param = nil
    c.recvq.enqueue(mysg)
    atomic.Store8(&gp.parkingOnChan, 1)
    gopark(chanparkcommit, unsafe.Pointer(&c.lock), waitReasonChanReceive, traceEvGoBlockRecv, 2)

    gp.waiting = nil
    success := mysg.success
    gp.param = nil
    mysg.c = nil
    releaseSudog(mysg)
    return true, success
}
```

（1）加锁；

（2）构造封装当前 goroutine 的 sudog 对象；

（3）完成指针指向，建立 sudog、goroutine、channel 之间的指向关系；

（4）把 sudog 添加到当前 channel 的阻塞读协程队列中；

（5）park 当前协程；

（6）倘若协程从 park 中被唤醒，则回收 sudog（sudog能被唤醒，其对应的元素必然已经被写入）；

（7）解锁，返回

### 4.6 读流程整体串联

![](https://pic3.zhimg.com/v2-26a783d36cc511f5d57706cb2076d526_1440w.jpg)

## 5 阻塞与非阻塞模式

在上述源码分析流程中，均是以阻塞模式为主线进行讲述，忽略非阻塞模式的有关处理逻辑.

此处阐明两个问题：

（1）非阻塞模式下，流程逻辑有何区别？

（2）何时会进入非阻塞模式？

### 5.1 非阻塞模式逻辑区别

非阻塞模式下，读/写 channel 方法通过一个 bool 型的响应参数，用以标识是否读取/写入成功.

（1）所有需要使得当前 goroutine 被挂起的操作，在非阻塞模式下都会返回 false；

（2）所有是的当前 goroutine 会进入死锁的操作，在非阻塞模式下都会返回 false；

（3）所有能立即完成读取/写入操作的条件下，非阻塞模式下会返回 true.

### 5.2 何时进入非阻塞模式

默认情况下，读/写 channel 都是阻塞模式，只有在 select 语句组成的多路复用分支中，与 channel 的交互会变成非阻塞模式：

```
ch := make(chan int)
select{
  case <- ch:
  default:
}
```

### 5.3 代码一览

```
func selectnbsend(c *hchan, elem unsafe.Pointer) (selected bool) {
    return chansend(c, elem, false, getcallerpc())
}

func selectnbrecv(elem unsafe.Pointer, c *hchan) (selected, received bool) {
    return chanrecv(c, elem, false)
}
```

在 select 语句包裹的多路复用分支中，读和写 channel 操作会被汇编为 selectnbrecv 和 selectnbsend 方法，底层同样复用 chanrecv 和 chansend 方法，但此时由于第三个入参 block 被设置为 false，导致后续会走进非阻塞的处理分支.

## 6 两种读 channel 的协议

读取 channel 时，可以根据第二个 bool 型的返回值用以判断当前 channel 是否已处于关闭状态：

```
ch := make(chan int, 2)
got1 := <- ch
got2,ok := <- ch
```

实现上述功能的原因是，两种格式下，读 channel 操作会被汇编成不同的方法：

```
func chanrecv1(c *hchan, elem unsafe.Pointer) {
    chanrecv(c, elem, true)
}

//go:nosplit
func chanrecv2(c *hchan, elem unsafe.Pointer) (received bool) {
    _, received = chanrecv(c, elem, true)
    return
}
```

## 7 关闭

![](https://pic2.zhimg.com/v2-46c42a11be76b1e553f5ee79acbcacab_1440w.jpg)

```
func closechan(c *hchan) {
    if c == nil {
        panic(plainError("close of nil channel"))
    }

    lock(&c.lock)
    if c.closed != 0 {
        unlock(&c.lock)
        panic(plainError("close of closed channel"))
    }

    c.closed = 1

    var glist gList
    // release all readers
    for {
        sg := c.recvq.dequeue()
        if sg == nil {
            break
        }
        if sg.elem != nil {
            typedmemclr(c.elemtype, sg.elem)
            sg.elem = nil
        }
        gp := sg.g
        gp.param = unsafe.Pointer(sg)
        sg.success = false
        glist.push(gp)
    }

    // release all writers (they will panic)
    for {
        sg := c.sendq.dequeue()
        if sg == nil {
            break
        }
        sg.elem = nil
        gp := sg.g
        gp.param = unsafe.Pointer(sg)
        sg.success = false
        glist.push(gp)
    }
    unlock(&c.lock)

    // Ready all Gs now that we've dropped the channel lock.
    for !glist.empty() {
        gp := glist.pop()
        gp.schedlink = 0
        goready(gp, 3)
  }
}
```

（1）关闭未初始化过的 channel 会 panic；

（2）加锁；

（3）重复关闭 channel 会 panic；

（4）将阻塞读协程队列中的协程节点统一添加到 glist；

（5）将阻塞写协程队列中的协程节点统一添加到 glist；

（6）唤醒 glist 当中的所有协程.

## 8 一道考题

要求实现一个 map：

（1）面向高并发；

（2）只存在插入和查询操作 O(1)；

（3）查询时，若 key 存在，直接返回 val；若 key 不存在，阻塞直到 key val 对被放入后，获取 val 返回； 等待指定时长仍未放入，返回超时错误；

（4）写出真实代码，不能有死锁或者 panic 风险.

```
type MyConcurrentMap struct{
  // ...
}

func (m *MyConcurrentMap)Put(k,v int){
  // ...
}

func (m *MyConcurrentMap)Get(k int, maxWaitingDuration time.Duration)(int,error){
  // ...
}
```

文末小广告：

欢迎老板们关注我的个人公众号：小徐先生的编程世界

我会不定期更新个人纯原创的编程技术博客，技术栈以 go 语言为主，让我们一起点亮更多的编程技能树吧！

编辑于 2023-01-07 09:43・北京