## 前菜： 服务器 能知道客户端取消了请求吗？

刚考完《计算机网络》的柳同学是这么回答的：哈？服务器检测客户端取消请求？但凡上过两天计算机网络课的人都知道 HTTP 是无状态的！服务器怎么可能知道上一个请求的客户端取没取消？

| HTTP 1.1 & 2（以 WPS 文档为例）                                                              | HTTP 3（以 Swift Forum为例）                                                             |
| ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| ![HTTP1.1 & 2](https://i-blog.csdnimg.cn/direct/bfee69daa43f43b6bd32454edb6b170e.png) | ![在这里插入图片描述](https://i-blog.csdnimg.cn/direct/8131e87cf3b7476db199046be9a66b24.png) |

HTTP请求之间无状态，这没毛病；但柳同学犯了个关键错误：他只盯着应用层的 HTTP 协议 ，把底下苦哈哈干活的传输层TCP/IP协议给忘了！无论是 HTTP/1.1 还是 HTTP/2，它们都基于 TCP；TCP是个有状态、面向连接的协议。服务器在处理当前这个正在进行的请求时，客户端要是关页面、断网、点刷新，底层这条TCP连接可就断了！

例如，对于.Net Web 应用处理资源密集型请求来说，为一个已经不在的客户端浪费 CPU 周期去发送一个昂贵的响应是毫无意义的，一个常见的技巧就是在开始执行资源密集型工作之前检查 `Response.IsClientConnected` \[[.NET](https://learn.microsoft.com/en-us/dotnet/api/system.web.httpresponse.isclientconnected)\]。

而在 Go 语言中， `http.Request.Context()` 返回一个与该 HTTP 请求生命周期绑定的 `context.Context` 对象。该上下文会在以下任一情况发生时被取消（即触发 `<-ctx.Done()` ）：

- 客户端主动关闭 TCP 连接（发 `FIN` 报文正常关闭 或 发 `RST` 报文异常终止）
- 在 HTTP/2 中，客户端显式通过 `RST_STREAM` 帧取消请求
- 服务端 `ServeHTTP` 方法返回，表示请求处理流程结束

这样服务端便有能力中断后续处理流程（如耗时任务、数据库查询、外部接口调用），避免资源浪费（及时释放 goroutine、关闭文件/连接等）。\[[Go](https://go.dev/doc/database/cancel-operations)\]

> 截止到笔者发布该博客，Go 标准库暂未原生支持 HTTP/3 / QUIC。  
> [https://github.com/golang/go/issues/32204](https://github.com/golang/go/issues/32204)

## 正菜：假如我们设计 Context 包

> What I cannot create, I do not understand.  
> ——Richard P. Feynman, 美国理论物理学家
### 场景需求

上下文（Context）可以应用于以下场景：

- **跨进程信息透传** ：如HTTP客户端的请求信息、Trace ID、请求时间戳等。
- **任务的级联取消** ：通过上下文可以统一控制关联任务是否继续执行，保证线程安全。此外，还允许任务设置超时或条件触发取消。

![任务级联取消示意图](https://i-blog.csdnimg.cn/direct/75a5b867dde246ff8cd07cc33205bd9a.png)

以如图所示树状结构为例，若需要取消父任务 `HandleReq1` ，我们希望取消信号能逐级传播，像 **多米诺骨牌效应** 一样迅速停止相关联的子任务（如数据库查询、文件操作或复杂计算等），从而及时回收服务器资源，避免浪费。

假如要我们来设计 context 包，我们要怎么做呢？首先，作为一个 标准库 ，对外应导出哪些函数？其次，如何设计行为接口，最后，如何设计数据结构并实现接口？

例如，你可以通过以下命令查看 **导出** （对外暴露的标识符，首字母大写）函数：

```bash
$ go doc context | grep "^func"
func AfterFunc(ctx Context, f func()) (stop func() bool)
func Cause(c Context) error
func WithCancel(parent Context) (ctx Context, cancel CancelFunc)
func WithCancelCause(parent Context) (ctx Context, cancel CancelCauseFunc)
func WithDeadline(parent Context, d time.Time) (Context, CancelFunc)
func WithDeadlineCause(parent Context, d time.Time, cause error) (Context, CancelFunc)
func WithTimeout(parent Context, timeout time.Duration) (Context, CancelFunc)
func WithTimeoutCause(parent Context, timeout time.Duration, cause error) (Context, CancelFunc)

```

其中 `With` 开头的用于根据 **父** 上下文派生一个 **子** 上下文（传值上下文、可取消上下文、超时上下文等）， `Cause` 则用于获取取消上下文的原因。

### 接口设计

面向上述需求与场景，本着最小化设计原则，我们来思考 `Context` 接口应该提供哪些方法：

1. **查询上下文状态的能力** ：上下文是否仍有效？任务还需继续执行吗？回忆 Go 语言的CSP模式并发的语法特性：当一个channel被关闭时， `select` 语句中对应的 `<-chan` 分支会立即执行，不再阻塞。因此，不妨设计一个 `Done()` 方法返回 `<-chan struct{}` ，借助此特性，可以优雅地通知长期执行的协程及时退出。
2. **上下文取消原因的辨识** ：当上下文被取消后，协程可能需要知道取消的具体原因（例如：父任务主动取消、超时触发、或其他情况）。因此，我们提供一个 `Cause() error` 或者 `Err() error` 方法，用以清晰地表明取消的原因或错误；如果上下文尚未结束，无错误。

> Cause 在英文中表示事件「起因」，是什么“导致”其发生。

3. **信息的跨任务传递（Key-Value）** ：上下文还需要传递某些额外的键值信息，不妨逻辑上做一个轻量的KV存储，通过 `Value(key any) any` 方法获取数据。

当然，由于键的 类 型为 `any` ，使用时需要类型断言。为避免频繁类型断言带来的问题，推荐设计类型安全的访问器（type-safe accessor）。例如说，与其写 `ctx.Value(userKey).(*User)` ，那不如封装一下：

```
type User struct {...}
type key int // 包内定义
var userKey key // 非导出，首字母小写

func LoadFromContext(ctx context.Context) (*User, bool) {
    u, ok := ctx.Value(userKey).(*User)
    return u, ok
}

```
4. **超时机制与截止时间** ：任务经常需要设置明确的截止时间以自动取消上下文。回忆 Go 语言多返回值的习惯用法，要不定义 `Deadline() (ddl time.Time, ok bool)` ？其中 `ok` 用以判断是否设置了截止时间。

查看源码，上述推导和思考简直和 Go 1.7标准库 中 Context 接口设计不谋而合，体现了Go语言一贯坚持的实用与简约哲学 **（bushi）** ：

```
type Context interface {
    Deadline() (deadline time.Time, ok bool)
    Done() <-chan struct{}
    Err() error
    Value(key any) any
}

```

### 接口实现：基础上下文

Go 不支持传统面向对象语言的继承机制，但通过组合（嵌入）可以实现类似的结构复用。我们先从最基础的上下文类型实现起：

```
type emptyCtx struct{}

func (emptyCtx) Deadline() (deadline time.Time, ok bool) {
    return
}

func (emptyCtx) Done() <-chan struct{} {
    return nil
}

func (emptyCtx) Err() error {
    return nil
}

func (emptyCtx) Value(key any) any {
    return nil
}

type backgroundCtx struct{ emptyCtx }

func (backgroundCtx) String() string {
    return "context.Background"
}

```

这两个是上下文树的“根”，不可被取消、不携带值、无超时机制。 `emptyCtx` 提供默认空实现。 `backgroundCtx` 通过嵌入复用其行为，仅实现自定义输出。

> `emptyCtx` 为啥是小写的（非导出）？是因为它是 **实现细节** ，只在 `context` 包内部使用，不需要暴露给外部用户。外部用户只需要通过导出的函数（如 context.Background()）来获取 emptyCtx 相关实例，不需要也不应该直接操作它。

> 在不少库中，用 `.Err()` 检查 来代替每步 `if err!=nil` 的 Error Check Hell.

### 接口实现：传值上下文

我们接下来实现三类具备实际功能的上下文： **传值** （ `valueCtx` ）、 **取消** （ `cancelCtx` ）和 **超时** （ `timerCtx` ）。它们都支持“父-子”嵌套结构，构成一条上下文链。

#### 数据结构 valueCtx

先从 `valueCtx` 开始。我们要解决的核心问题是：上下文如何 存储 数据？一种自然的想法是用一个哈希表来存储多个 key-value 对。但这样做有点过度了 —— 上下文设计的初衷是 **传递少量、只读的元信息** ，比如 userID、traceID，不是用来做数据容器。而且， `val` 可以是任意类型，包括结构体。因此，包装一个额外的 `key-value` 对就足矣：

```
type valueCtx struct {
    Context
    key, val any
}
```

> 这个 `Context` 是匿名嵌入的父上下文，Go 会自动将其方法“提升”，对外表现为链式结构。

#### 构造方法 WithValue

接下来实现 `WithValue()` 方法，它的作用是：在原有上下文基础上，附加（包装）一个新的键值对，返回一个新的上下文。 **有没有一点像装饰器模式。**

```
func WithValue(parent Context, key, val any) Context {
    // 不允许 nil 上下文或键
    if parent == nil {
        panic("cannot create context from nil parent")
    }
    if key == nil {
        panic("nil key")
    }
    // 键必须是可比较的（底层用 == 比较）
    if !reflectlite.TypeOf(key).Comparable() {
        panic("key is not comparable")
    }
    // 返回值仍是 Context 接口，保证链式封装
    return &valueCtx{parent, key, val}
}

```

> 这里返回的是 &valueCtx{…} ，为什么是指针？上下文本身是链式结构，而且传递开销低，且传递后，接收指针的函数 / 方法体中依然可以修改指针指向的内存单元的值。

#### 链式查找（递归逻辑 + 循环实现）

由于一个 `valueCtx` 只存储一个 key-value 对，若要支持跨多层上下文的值传递，就必须能够 **沿着上下文链向父节点查找** 。因此，我们实现 `Value()` 方法时，需要检查当前节点是否匹配，如果不匹配则递归查找父节点。

```
func (c *valueCtx) Value(key any) any {
    if c.key == key {
        return c.val
    }
    // 找不到就找父亲
    return value(c.Context, key)
}

```

调用一个封装的私有函数 `value()` ，它负责沿上下文链逐层查找：

```
// 基于源码的变式，便于理解
func value(c Context, key any) any {
    switch ctx := c.(type) {
    case *valueCtx:
        if key == ctx.key {
            return ctx.val
        }
        return value(ctx.Context, key) // 继续向父节点查找
    ... // 还有一些 case 我们还没谈到，例如计时器上下文
    case backgroundCtx: // 根节点，查找结束
        return nil
    default:
        return c.Value(key)
    }
}
```

> 这种查找机制本质上是对 **链表** 的一次单链向上遍历。

长链调用用递归会导致栈帧增长，下文常用于高并发场景，必须保证堆栈浅、调用快。因此，我们把递归改为使用的是 `for` 循环。

```
// 真正的源码（节选）
func value(c Context, key any) any {
    for {
        switch ctx := c.(type) {
        case *valueCtx:
            if key == ctx.key {
                return ctx.val
            }
            c = ctx.Context // 下一轮循环继续向父节点查找
        ... // 还有一些 case 我们还没谈到，例如计时器上下文
        case backgroundCtx: // 根节点，查找结束
            return nil
        default: // 兼容自定义 Context 实现
            return c.Value(key)
        }
    }
}
```

### 接口实现：可取消上下文

前面我们完成了传值功能，接下来分析如何实现上下文 ==树== 的“级联取消”，父节点向子节点传播取消信号。一个父任务被取消时，能自动取消所有子任务。

#### 行为接口 canceler

我们首先来定义一个行为接口。一个“可取消的上下文”，除了基础上下文方法，还应该具备一个能力 —— 触发取消自身。既然如此，我们不妨抽象出一个接口 `canceler` ，表示可以 **被取消的东西** ；这个接口内部试用即可，外部不会暴露。它至少要能：判断自己是否已被取消 `Done()` ；能够主动取消自己，并传播取消信号给子上下文 `cancel()` ：

```
type canceler interface {
    cancel(removeFromParent bool, err error)
    Done() <-chan struct{}
}
```

> 顺带一提实现了 `String()` 方法就是实现了 `stringer` 接口。

#### 并发基础回顾：计数器

在深入可取消上下文的源码前，先用一段极简的计数器示例帮你复习竞态条件、 `goroutine` 、 `mutex` 与 `atomic` 。如果你已对这块胸有成竹，可直接跳过。

竞争问题是并发编程中一个常见且棘手的问题。你可以运行下面的代码，观察其输出结果：

```
import (
    "fmt"
    "sync"
    "sync/atomic"
    "testing"
)

func TestCounter(t *testing.T) {
    var counter int32     // 计数值
    var wg sync.WaitGroup // 等待 goroutine 执行完毕

    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for i := 0; i < 100; i++ {
                counter++ // 非原子操作，存在竞争条件
            }
        }()
    }
    wg.Wait()
    fmt.Println(counter)
}

```

`counter++` 不是一个 ==原子操作== ，它实际包含以下几个步骤：

```go
tmp := counter
tmp = tmp + 1
counter = tmp
```

那么如何应付这个竞争问题呢？常见的两种方式是使用 `mutex` 或 `atomic` 原语。首先是使用 `Mutex` 加锁：

```go
import (
    "fmt"
    "sync"
    "sync/atomic"
    "testing"
)

type Counter struct {
    mu      sync.Mutex
    counter int32
}

// 使用互斥锁保护临界区
func (c *Counter) Increment() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.counter++
}

func (c *Counter) String() string {
    return fmt.Sprintf("%d", c.counter)
}

func TestCounterMutex(t *testing.T) {
    var counter Counter
    var wg sync.WaitGroup

    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func() {
            for i := 0; i < 100; i++ {
                counter.Increment()
            }
            wg.Done()
        }()
    }

    wg.Wait()
    fmt.Println(counter.String())
}

```

然后是使用 `atomic` 原语：

```go
import (
    "fmt"
    "sync"
    "sync/atomic"
    "testing"
)

func TestCounterAtomic(t *testing.T) {
    var data atomic.Int32
    var wg sync.WaitGroup

    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for i := 0; i < 100; i++ {
                data.Add(1)
            }
        }()
    }
    
    wg.Wait()
    fmt.Println(data.Load())
}

```

你可以运行 `benchmark` 测试查看两种实现方法的性能差异：

> 代码🌟： [GitHub](https://github.com/DURUII/golang-from-scratch/blob/main/advanced/01%20%E5%B9%B6%E5%8F%91%E7%BC%96%E7%A8%8B/counter/counter_test.go)

```bash
(base) ➜  counter git:(dev) ✗ go test -bench=Bench -run=^$  
goos: darwin
goarch: arm64
pkg: advanced/ch01/counter
cpu: Apple M1
BenchmarkCounterMutex-8              990           1374837 ns/op
BenchmarkCounterAtomic-8            2809            403374 ns/op
PASS
ok      advanced/ch01/counter   2.823s
```

从结果来说，原子操作远快于互斥锁（约快 3.4 倍），使用互斥锁实现，每次操作平均耗时约 **1.374 毫秒** ；使用原子操作实现，每次操作平均耗时约 **0.403 毫秒** 。

> 结论：如果只是并发读写单个变量，没有复杂的资源竞争需要锁保护整个代码区或对象，用 `atomic` 就够了。它直接利用了 CPU 提供的原子指令，不需要加锁，也避免了锁的开销和可能的上下文切换，性能更高。

#### 数据结构 cancelCtx

我们需要一个结构体，维护取消的状态（如为何取消，需要 `err` 或者 `cause` ），需要维护 **树状结构** （比如有哪些子上下文要一并取消）。为了线程安全，我们还要考虑并发访问这些状态的互斥控制。

取消一个父 `Context` ，所有从它派生出来的子 `Context` 会被自动取消，肯定需要一个 `children` 集合；要实现取消信号传播，可以用到前文提到的。

```go
type cancelCtx struct {
    Context

    mu       sync.Mutex
    done     atomic.Value  // 懒加载通道类型，取消后关闭通道
    children map[canceler]struct{} // 取消后重置为 nil
    err      error  // 取消前为 nil，取消后有值
}
```

> 用 `atomic.Value` 的原因很简单：它读写无锁、能保证可见性，适合“懒加载”一个只写一次、之后只读的 `chan struct{}` 。  
> 那为什么还需要 sync.Mutex 呢？我们来想一下：
> 
> - 关闭通道只能有一次：即使 done 的指针通过 atomic 可见，多个 goroutine 仍可能同时尝试 close(ch)，必须用锁避免同一个通道被多次 `close` 。
> - 一组动作的原子事务：取消要关闭通道、设置 `err/cause` 、遍历 children 并递归取消，这几步要么全部完成，要么全部不发生；锁把整段逻辑包成临界区。
> - 并发读写的 map：新增子节点、删除子节点、遍历子节点都需要互斥，否则会出现 map 并发读写 panic。
> 
> 因此， `atomic.Value` 是对 `done` 这个字段的读取；状态一致性靠 `sync.Mutex` 。

#### 构造方法 WithCancel

联想到 **观察者** 模式，这里的 `children` 就是 `List<Observer> observers ` 。 ![在这里插入图片描述](https://i-blog.csdnimg.cn/direct/8326094ca5ad4e0887061e565de22c19.png)

```go
var Canceled = errors.New("context canceled")

// closedchan is a reusable closed channel.
var closedchan = make(chan struct{})

func init() {
    close(closedchan)
}

func (c *cancelCtx) cancel(removeFromParent bool, err error) {
    if err == nil {
        panic("context: internal error: missing cancel error")
    }
    c.mu.Lock()
    // already canceled
    if c.err != nil {
        c.mu.Unlock()
        return
    }
    c.err = err
    // cancel closes c.done
    d, _ := c.done.Load().(chan struct{})
    if d == nil {
        c.done.Store(closedchan)
    } else {
        close(d)
    }
    // cancels each of c's children [notifyAllObservers]
    for child := range c.children {
        // NOTE: acquiring the child's lock while holding parent's lock.
        child.cancel(false, err, cause)
    }
    c.children = nil
    c.mu.Unlock()
    // ...
}

func WithCancel(parent Context) (ctx Context, cancel CancelFunc) {
    if parent == nil {
        panic("cannot create context from nil parent")
    }
    
    child := &cancelCtx{Context: parent}
    // ----- propagateCancel -----
    // 1. 父上下文无法取消
    if parent.Done() == nil {
        return child, func() { child.cancel(true, Canceled, nil) }
    }

    // 2. 无锁，父已经取消，立即取消 child
    select {
    case <-parent.Done():
        child.cancel(false, parent.Err(), nil)
        return child, func() { child.cancel(true, Canceled, nil) }
    default:
    }

    // 3. 如果父是 *cancelCtx
    p, ok := parent.(*cancelCtx)
    if ok {
        p.mu.Lock()
        // 二次检查
        if p.err != nil {
            child.cancel(false, p.err, p.cause)
        } else {
            if p.children == nil {
                p.children = make(map[canceler]struct{})
            }
            // 把 child 注册到 children 里
            p.children[child] = struct{}{}
        }
        p.mu.Unlock()
        return child, func() { child.cancel(true, Canceled, nil) }
    }

    return child, func() { child.cancel(true, Canceled, nil) }
}

```

你可能会问： `p.err != nil` 和 `case <-parent.Done():` 不是重复的么。

- 第 1 步（select）：
	- 父 Done 已经关闭 → 直接取消 child，return；
		- 父 Done 未关闭 → 继续往下走。
- 第 2 步（加锁后 if）：
	- 拿到锁后再次看 p.err（ **因为在这两步之间就“说时迟那时快”可能有别的 goroutine把父节点取消了** ）；
		- 若 err!= nil：父已取消 → 立即取消 child；
		- 否则：把 child 加入 p.children，完成注册。

> 先无锁快速判断，减少因无锁带来的不必要的性能损耗；再加锁二次确认，把临界区缩到最小。

#### 特别地：超时上下文 timerCtx

实际上，把 `canceler` 与 `timer` 这两个能力拼装在一起，就得到了超时上下文 `timerCtx` ，重复内容不再赘述。

其对外暴露的构造方法是 `WithDeadline` （ `WithTimeout` 是把 `timeout` 转成 `deadline` 再调它）。

```
func WithDeadlineCause(parent Context, d time.Time error) (Context, CancelFunc) {
    // ...
    if c.err == nil {
        c.timer = time.AfterFunc(dur, func() {
            // 传错误/原因是超时
            c.cancel(true, DeadlineExceeded)
        })
    }
    return c, func() { c.cancel(true, Canceled) }
}
go运行12345678910
```

#### 取消信号传递的兜底方案

此外， `context` 包还有一个 **兜底方案** ，当父 `context` 既不是 `*cancelCtx` ，也没有实现 `afterFuncer` 时，就用独立 goroutine 去“监听”父 context 的完成事件，并把取消信号转发给当前 child。

```
func (c *cancelCtx) propagateCancel(parent Context, child canceler) {
    // ...
    goroutines.Add(1)
    go func() {
        select {
        case <-parent.Done():
            child.cancel(false, parent.Err(), Cause(parent))
        case <-child.Done():
        }
    }()
}
go运行1234567891011
```

还记得场景需求分析里的这一句话吗？当一个channel被关闭时， `select` 语句中对应的 `<-chan` 分支会立即执行，不再阻塞。

注意，在 **自己的业务代码** 里，我们当然可以随手启动一个 goroutine 去监听 `parent.Done()` ，看起来既直观又省事。但 **context 包是基础设施** ，它面对的是整个 Go 生态里所有高并发服务：一次请求就可能衍生出成百上千个子 Context。如果每个父子关系都额外启动一个 goroutine，光是“监听父 Done”这件事就会把机器吃光。

> observers + notifyAllObservers v.s. children + cancelAllChildren

因此，标准库采用了 **观察者模式** ： 父节点内部维护一个 `children set` ，取消时遍历直接调用子节点的 `cancel()` 方法—— **零额外 goroutine、零调度开销、生命周期可控** 。 只有当父节点是用户自定义类型（非 `*cancelCtx` ）时，才退而求其次，临时启动一个 goroutine 做兜底兼容。

> 注：本节在尽量保留 [context](https://go.dev/src/context/context.go) 原始实现的同时，为了兼顾了读者阅读体验，仍做了适度精简。建议在读完本节之后再回头看源码，相信你会更容易理解其中的设计取舍与实现细节，更能体会它是如何内部优雅处理协程取消、超时控制等并发场景下的复杂问题。

## 外带：代码示例

> Talk is cheap. Show me the code.  
> ——Linus Torvalds, Linux 之父, 赫尔辛基大学计算机系

### 级联任务取消

```
import (
    "context"
    "errors"
    "fmt"
    "github.com/stretchr/testify/assert"
    "runtime"
    "testing"
    "time"
)

var ErrTooHot = errors.New("太热了，机器热化了")

// 关联任务的取消（级联退出）
// context 用于管理相关任务的上下文，包含了共享值的传递，超时，取消通知
func isCanceled(ctx context.Context) bool {
    select {
    case <-ctx.Done():
        return true
    default:
        return false
    }
}

func TestCancelByContext(t *testing.T) {
    // 父节点
    ctx, cancel := context.WithCancelCause(context.Background())
    for i := 0; i < 5; i++ {
        go func(ctx context.Context, i int) {
            for {
                if isCanceled(ctx) {
                    fmt.Println(i, "下班了，因为", context.Cause(ctx))
                    break
                }
                fmt.Println(i, "在工作")
                time.Sleep(5 * time.Millisecond)
            }
        }(ctx, i)
    }
    // 广播机制
    cancel(ErrTooHot)
    time.Sleep(time.Second * 1)
    // 确保没有协程泄漏
    assert.Equal(t, runtime.NumGoroutine(), 2)
}

```

你可以在CSDN代码框右上角点击运行按钮查看运行结果。下面是一种可能的输出（下班打印次序不固定）：

```bash
=== RUN   TestCancelByContext
4 下班了，因为 太热了，机器热化了
1 下班了，因为 太热了，机器热化了
2 下班了，因为 太热了，机器热化了
3 下班了，因为 太热了，机器热化了
0 下班了，因为 太热了，机器热化了
--- PASS: TestCancelByContext (1.00s)
PASS
```

> 当然，在这个特定的例子下，还可以用 close(chan) 的方式优雅取消全部任务。

### HTTP 超时控制

> 代码： [Github](https://github.com/DURUII/golang-from-scratch/tree/main/advanced/01%20%E5%B9%B6%E5%8F%91%E7%BC%96%E7%A8%8B/timeout)

假如你是OCR服务端，客户端（用户）向你发送图片转文字的请求，你做鉴权、超时、分发调度等等，而你向AI中台（或腾讯云/ 阿里云 /京东云文字识别 OCR 服务）发送请求，依靠他们的能力。

- 假如用户手动取消，比如关闭浏览器或取消按钮，那么相关任务应该级联取消
- 假如 AI 服务崩了，一直不返回请求，那么设置的超时时间到了也要主动放弃请求取消相关任务

如何定位“谁取消了请求”？就可以用到 `Cause()` 或者 `Err()` 方法。

```
import (
    "github.com/gin-gonic/gin"
    "net/http"
    "strconv"
    "time"
)

type Response struct {
    Code int         \`json:"code"\`
    Msg  string      \`json:"msg"\`
    Data interface{} \`json:"data,omitempty"\`
}

func NewRouter() *gin.Engine {
    gin.SetMode(gin.ReleaseMode)
    r := gin.New()
    r.GET("/work", func(c *gin.Context) {
        t, _ := strconv.Atoi(c.DefaultQuery("t", "0"))
        time.Sleep(time.Duration(t) * time.Millisecond)
        c.JSON(http.StatusOK, Response{Msg: "success"})
    })
    return r
}
go运行1234567891011121314151617181920212223
```
```
import (
    "context"
    "encoding/json"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "net/http"
    "testing"
    "time"
)

func TestHTTPClientSuccess(t *testing.T) {
    // service setup
    go func() {
        _ = NewRouter().Run(":8080")
    }()
    time.Sleep(50 * time.Millisecond)

    // request
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    req, err := http.NewRequestWithContext(ctx, http.MethodGet,
        "http://127.0.0.1:8080/work?t=100", nil)
    require.NoError(t, err)

    // response
    resp, err := (&http.Client{Timeout: 500 * time.Millisecond}).Do(req)
    require.NoError(t, err)

    var res Response
    err = json.NewDecoder(resp.Body).Decode(&res)
    require.NoError(t, err)
    require.Equal(t, Response{Msg: "success"}, res)
}

func TestHTTPClientDeadlineExceeded(t *testing.T) {
    // service setup
    go func() {
        _ = NewRouter().Run(":8080")
    }()

    // request
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    req, err := http.NewRequestWithContext(ctx, http.MethodGet,
        "http://127.0.0.1:8080/work?t=800", nil)
    require.NoError(t, err)

    // 超时
    _, err = (&http.Client{Timeout: 1 * time.Millisecond}).Do(req)
    require.Error(t, err)
    assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestHTTPClientContextCanceled(t *testing.T) {
    // service setup
    go func() {
        _ = NewRouter().Run(":8080")
    }()
    time.Sleep(50 * time.Millisecond)

    // request
    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        time.Sleep(350 * time.Millisecond)
        cancel()
    }()

    req, err := http.NewRequestWithContext(ctx, http.MethodGet,
        "http://127.0.0.1:8080/work?t=800", nil)
    require.NoError(t, err)

    // response
    _, err = (&http.Client{Timeout: 500 * time.Millisecond}).Do(req)
    require.Error(t, err)
    assert.ErrorIs(t, err, context.Canceled)
}

```

### HTTP 请求取消

最后，回到最开始的例子：柳同学还不甘心写代码验证，用 `<-c.Request.Context().Done()` 写代码验证，打开浏览器访问 `/test` 后立刻关闭标签页，日志果然打印 `context canceled` 。

```
func NewServer() *gin.Engine {
    r := gin.New()

    r.GET("/test", func(c *gin.Context) {
        ctx := c.Request.Context()
        select {
        case <-time.After(1 * time.Hour): // 模拟长任务
            c.JSON(http.StatusOK, gin.H{"msg": "success"})
        case <-ctx.Done(): // 请求取消
            fmt.Println("request canceled:", ctx.Err())
            return
        }
    })

    return r
}

```