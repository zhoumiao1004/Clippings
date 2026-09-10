
> **context 三部曲 · 壹** 。本系列基于 2026 年 8 月发布的 Go 1.27，全部示例在 go1.27.0（linux/amd64）下实际编译运行，文中给出的就是真实输出。第二篇讲取消原因与工程实战，第三篇讲测试、源码与 agent 流式取消。

服务的内存曲线一天天往上爬，重启就好，过几天又爬回来——这种事故的常见元凶，是 **卡在 channel 上再也醒不过来的 goroutine** 。它们大多来自同一个根源：写了并发代码，却没有把 `context` 用对。

以前定位这种泄漏要靠对比多份 goroutine dump 猜。Go 1.27 把它变成了一条命令的事，本文后半段会当场演示。但工具只能告诉你"漏了"，不漏的写法要靠正确的心智模型——这正是这篇要建立的东西。

一句话版本：

> `context.Context` 是一种跨函数、跨 goroutine、跨 API 边界传递取消信号、截止时间和请求级数据的标准协议。它不是调度器，也不会强制杀掉任何 goroutine——它只发"该停了"的信号，怎么停、何时停，由下游自己配合完成。

## 为什么 2026 年了还要重新学 context

因为网上大部分 context 文章停留在 Go 1.19 之前的语义。先看一张图校准时间线：

![](https://pic4.zhimg.com/v2-e1ce35ef818c2d5b802e119fd0f7a187_1440w.jpg)

两个关键事实： **核心 API 在 Go 1.21 就已定型** ，从 1.21 到 1.27，context 包没有新增任何导出 API，所以要学的东西并不多；而 **近三年真正的变化在配套** ——测试假时钟（synctest）、泄漏检测（goroutineleak profile）、信号归因（NotifyContext 的 Cause）。旧文章不是错在 API，而是错过了这些让 context 真正好用起来的东西。

## context 到底解决什么问题

一个用户请求，往往会扇出一串下游工作：查库、调远程 API、起子 goroutine、写日志审计。用户断开连接、请求超时、上游取消时，这些工作不该继续白跑。

![](https://pic2.zhimg.com/v2-f7e8b876f0c5643200acb94751deaa9f_1440w.jpg)

如果只用裸的 `done channel` ，简单场景够用，但复杂系统立刻会遇到一串问题：如何表达 deadline？如何表达取消原因？父任务取消后怎么自动取消所有子任务？某个子任务取消时怎么不影响父任务和兄弟任务？HTTP、数据库、RPC 这些标准库和生态库怎么统一接收取消信号？

`context` 就是 Go 对这些问题的标准答案。

## 只有四个方法的接口

```go
type Context interface {
  Deadline() (deadline time.Time, ok bool)
  Done() <-chan struct{}
  Err() error
  Value(key any) any
}
```

典型用法就是一个 select：

```go
select {
case <-ctx.Done():
  return ctx.Err()
case result := <-work:
  return result
}
```

两个容易忽略的细节： `Done()` 返回的 channel 在取消时是 **被关闭** ，不是收到一个值；永不取消的 context（Background、TODO）的 `Done()` 返回 **nil** ——从 nil channel 接收会永久阻塞，所以上面的 select 依然正确，只是那个分支永远选不中。

## 最重要的一件事：取消是协作式的

调用 `cancel()` 不会强制停止任何 goroutine，它只是关闭 `ctx.Done()` 。下游代码必须主动监听这个信号，才会退出：

```
上游：我不需要这个结果了，你可以停了。
下游：我收到信号，在合适的位置退出。
```

如果下游代码完全不检查 `ctx.Done()` ，也没有把 ctx 传给任何支持 context 的 API，context 就无法让它停止。后面所有示例、所有坑，都是这句话的展开。

## Background、TODO 和父子链

最顶层的 context 来自 `context.Background()` （明确的根节点：main、初始化、server 入口）或 `context.TODO()` （暂时不知道该传什么的占位）。两者行为完全一样：不可取消、无 deadline、无 value，区别只是语义，方便静态分析工具和读代码的人识别"这里还欠一个 ctx"。测试代码里 Go 1.24 起有更好的选择 `t.Context()` ，第三篇细讲。

其余 context 都从 parent 派生：

```
ctx, cancel := context.WithCancel(parent)
ctx, cancel := context.WithTimeout(parent, time.Second)
ctx, cancel := context.WithDeadline(parent, deadline)
ctx = context.WithValue(parent, key, value)
```

派生出来的是一棵树，取消传播的规则只有一条方向：

![](https://pica.zhimg.com/v2-e70108378a31a6f521d0f65950776ab6_1440w.jpg)

下面用八个都能直接 `go run` 的示例把这些规则变成手感。运行方式：新建目录， `go.mod` 写 `module ctxdemo` + `go 1.27` ，示例存成 `main.go` ， `go run .`。 `go` 指令一定要写 1.27——Go 1.27 起 `go test` 默认会跑 vet 的 stdversion 检查，版本声明低了会直接报错。

## 示例一：最基本的 WithCancel

主 goroutine 调用 `cancel()` ，子 goroutine 通过 `ctx.Done()` 感知取消后主动退出。

```
package main
​
import (
  "context"
  "fmt"
  "time"
)
​
func worker(ctx context.Context) {
  ticker := time.NewTicker(300 * time.Millisecond)
  defer ticker.Stop()
​
  for {
    select {
    case <-ctx.Done():
      fmt.Println("worker stopped:", ctx.Err())
      return
    case <-ticker.C:
      fmt.Println("working...")
    }
  }
}
​
func main() {
  ctx, cancel := context.WithCancel(context.Background())
​
  done := make(chan struct{})
  go func() {
    defer close(done)
    worker(ctx)
  }()
​
  time.Sleep(time.Second)
  cancel()
​
  <-done
  fmt.Println("main exit")
}
```

实际运行输出：

这里有两个重点。

第一， `cancel()` 没有杀掉 goroutine，只是关闭了 `ctx.Done()` ； `worker` 是在 select 中发现取消信号后自己 return 的。

第二，两个写法习惯值得养成：周期性工作用 Ticker 放进 **同一个 select** （另一种常见写法 `default: 干活; time.Sleep(300ms)` 也能跑，但取消信号最多要等一个 Sleep 周期才被发现）； `main` 用 done channel 等 worker **真正退出** ，而不是再 Sleep 一段时间去猜——用户看到"已停止"时，后台必须真的停了。

## 示例二：避免 goroutine 泄漏

context 最常见的用途之一：调用方不再消费数据时，通知生产者退出。

```
package main
​
import (
  "context"
  "fmt"
)
​
func gen(ctx context.Context) <-chan int {
  out := make(chan int)
​
  go func() {
    defer close(out)
​
    for i := 1; ; i++ {
      select {
      case <-ctx.Done():
        return
      case out <- i:
      }
    }
  }()
​
  return out
}
​
func main() {
  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()
​
  for n := range gen(ctx) {
    fmt.Println(n)
    if n == 5 {
      break
    }
  }
​
  cancel()
  fmt.Println("done")
}
```

输出是 1 到 5 然后 done。关键在生产者的这个 select：

```
select {
case <-ctx.Done():
  return
case out <- i:
}
```

`out <- i` 是可能阻塞的操作。调用方提前 break 后就没人读 `out` 了，生产者如果没监听 `ctx.Done()` ，会永远卡在发送上。所以规则是：

顺带说两个细节： `CancelFunc` 可以被多个 goroutine 同时调用、也可以重复调用，第一次之后都是空操作，所以 `defer cancel()` 之外再提前调一次没有问题；示例里的 `cancel()` 除了通知 goroutine 退出，还负责释放 WithCancel 创建的内部资源（第三篇讲源码时会看到具体是什么）。

## 示例三：用 Go 1.27 把泄漏的 goroutine 揪出来

把示例二的 `gen` 去掉 ctx，就是一个标准的泄漏现场。Go 1.26 实验、Go 1.27 正式提供的 `goroutineleak` profile 可以发现这类泄漏：goroutine 阻塞在某个并发原语上，而这个原语已经不可能再被任何可运行的 goroutine 唤醒。

```
package main
​
import (
  "fmt"
  "os"
  "runtime/pprof"
  "time"
)
​
func leakyGen() <-chan int {
  out := make(chan int)
​
  go func() {
    for i := 1; ; i++ {
      out <- i
    }
  }()
​
  return out
}
​
func main() {
  for n := range leakyGen() {
    fmt.Println(n)
    if n == 3 {
      break
    }
  }
​
  time.Sleep(100 * time.Millisecond)
​
  profile := pprof.Lookup("goroutineleak")
  if profile == nil {
    panic("goroutineleak profile is unavailable")
  }
​
  _ = profile.WriteTo(os.Stdout, 1)
}
```

实际运行输出：

![](https://pic4.zhimg.com/v2-baba8ef377e53d4811eaea99e8478867_1440w.jpg)

调用方读到 3 就退出循环，从此没人接收 `out` ，生产者永久阻塞在第 15 行的 `out <- i` 上——profile 直接把行号指给你。把 `WriteTo` 第二个参数改成 `2` ，会以 panic 堆栈格式输出，泄漏的 goroutine 被标成 `goroutine N [chan send (leaked)]` 。线上服务引入 `net/http/pprof` 后，访问 `/debug/pprof/goroutineleak` 拿到同样的结果。

一个边界要知道：它的判定依据是 **可达性** 。第三篇会有一个"错误 select 写法"的示例，goroutine 卡在 `<-ch` 上，但只要 main 还持有 `ch` ，理论上就还可能被唤醒，profile 不会把它算作泄漏。

## 示例四：Go 1.23 迭代器——不起 goroutine 就没有泄漏

示例二那种"goroutine + channel"生产者并不总是必要。Go 1.23 的 range-over-func 迭代器让很多同步生产场景可以既不要 goroutine 也不要 channel：

```
package main
​
import (
  "context"
  "fmt"
  "iter"
  "time"
)
​
func gen(ctx context.Context) iter.Seq[int] {
  return func(yield func(int) bool) {
    for i := 1; ; i++ {
      if ctx.Err() != nil {
        return
      }
      if !yield(i) {
        return
      }
    }
  }
}
​
func main() {
  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()
​
  for n := range gen(ctx) {
    fmt.Println(n)
    if n == 5 {
      break
    }
  }
​
  fmt.Println("done")
}
```

输出同样是 1 到 5 加 done。调用方 break 时 `yield` 返回 false，生产函数直接结束，"发送方没人接收"的问题从根上不存在。选择标准：同步计算、分页读取、逐行扫描优先用迭代器；生产者需要真正并发运行（预取、流水线、多路合并）时才用 goroutine + channel + ctx。注意迭代器版本仍然保留了 ctx 参数——上游取消时它照样尽早停。

## 示例五、六：超时与截止时间

`WithTimeout` 限制任务最多执行多久：

```
package main
​
import (
  "context"
  "fmt"
  "time"
)
​
func slowOperation(ctx context.Context) error {
  select {
  case <-time.After(2 * time.Second):
    fmt.Println("operation completed")
    return nil
  case <-ctx.Done():
    return ctx.Err()
  }
}
​
func main() {
  ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
  defer cancel()
​
  err := slowOperation(ctx)
  fmt.Println("result:", err)
}
```

输出：

```
result: context deadline exceeded
```

`slowOperation` 需要 2 秒，ctx 800ms 超时，所以 select 先走到 `ctx.Done()` 分支。再强调一遍协作式：如果它写成 `time.Sleep(2 * time.Second)` ，超时 **不会** 打断 Sleep，因为没人检查信号。心智模型是三段式——WithTimeout 设截止时间，ctx.Done() 发信号，下游配合检查。

`WithDeadline` 是绝对时间点版本， `WithTimeout(parent, d)` 在标准库里就是 `WithDeadline(parent, time.Now().Add(d))` ：

```
package main
​
import (
  "context"
  "fmt"
  "time"
)
​
func main() {
  deadline := time.Now().Add(500 * time.Millisecond)
​
  ctx, cancel := context.WithDeadline(context.Background(), deadline)
  defer cancel()
​
  select {
  case <-time.After(time.Second):
    fmt.Println("work finished")
  case <-ctx.Done():
    fmt.Println("stopped:", ctx.Err())
  }
}
```

输出 `stopped: context deadline exceeded` 。两者的取消条件相同：手动 cancel、父取消、到期，谁先发生谁生效。deadline 是绝对时间的好处是可以跨边界传递——gRPC 就把它折算成 `grpc-timeout` 头发给对端，对端据此建立自己的 context，每一层用的都是"剩余时间"。

另外，示例五 select 里的 `time.After` 在 Go 1.23 之后没有泄漏问题了（未触发且不可达的 timer 会被 GC），高频循环里若在意分配，可以复用 `time.Timer` 。

## 示例七、八：父子传播的两个方向

父取消，子全跟着：

```
package main
​
import (
  "context"
  "fmt"
  "sync"
)
​
func watch(ctx context.Context, name string) {
  <-ctx.Done()
  fmt.Println(name, "stopped:", ctx.Err())
}
​
func main() {
  parent, cancelParent := context.WithCancel(context.Background())
​
  child1, cancelChild1 := context.WithCancel(parent)
  defer cancelChild1()
​
  child2, cancelChild2 := context.WithCancel(parent)
  defer cancelChild2()
​
  var wg sync.WaitGroup
  wg.Go(func() { watch(child1, "child1") })
  wg.Go(func() { watch(child2, "child2") })
​
  cancelParent()
  wg.Wait()
}
```

输出两行顺序不固定（goroutine 调度顺序不确定）：

```
child2 stopped: context canceled
child1 stopped: context canceled
```

`wg.Go` 是 Go 1.25 的新方法，等价于 `wg.Add(1)` + `go func() { defer wg.Done(); ... }()` 。注意 `watch` 把 ctx 放在第一个参数，这是 Go 的通用约定。

反过来，子取消不影响父和兄弟：

```
package main
​
import (
  "context"
  "fmt"
  "time"
)
​
func main() {
  parent, cancelParent := context.WithCancel(context.Background())
  defer cancelParent()
​
  child1, cancelChild1 := context.WithCancel(parent)
  child2, cancelChild2 := context.WithCancel(parent)
  defer cancelChild2()
​
  cancelChild1()
​
  time.Sleep(100 * time.Millisecond)
​
  fmt.Println("parent err:", parent.Err())
  fmt.Println("child1 err:", child1.Err())
  fmt.Println("child2 err:", child2.Err())
}
```

输出：

```
parent err: <nil>
child1 err: context canceled
child2 err: <nil>
```

这个单向性非常重要： `cancelChild1()` 只表示"child1 这份工作不做了"，不表示"整个请求失败了"。 **"任意子任务失败就取消整个父任务"是业务决策，不是 context 的默认行为** ——需要在父层建 `WithCancelCause` ，由失败的子任务主动调父层的 cancel。第三篇的"首错取消"示例就是这套编排的完整实现，十几行标准库代码。

## 第一篇小结：三条军规

一、context 是协作式取消：cancel 只发信号，下游必须在 select 里监听 `ctx.Done()` ，或把 ctx 传给支持它的 API；所有可能阻塞的收、发、等待，都和 `ctx.Done()` 放进同一个 select。

二、取消只向下传播：父取消波及全部子孙，子取消不惊动任何人；跨方向的联动是业务编排，要自己写。

三、生产者必须能被叫停：goroutine + channel 就监听 ctx，能不并发就用 Go 1.23 迭代器；怀疑漏了，Go 1.27 的 goroutineleak profile 一条命令定位。

下一篇进入工程深水区：线上日志只剩一句 `context canceled` ，到底是谁取消的？Go 1.20 的 Cause 让取消"有名有姓"，但 `WithTimeoutCause` 有一个几乎人人踩过的陷阱—— **你写的超时原因可能从未生效** 。我们还会把 request id 无侵入地贯穿中间件、业务代码和 slog 日志，并给出 HTTP 服务优雅退出的完整时序。

---

*本系列基于 Go 1.27.0（linux/amd64）实测，全部示例可直接运行。*

*系列目录：*

*壹（本篇）·*

*贰 取消要有名有姓 ·*

*叁 高阶与源码。*

*示例编号全系列连续，本篇为示例一～八。*

[所属专栏 · 2026-08-31 11:36 更新](https://zhuanlan.zhihu.com/c_2076990667423859580)

[![](https://picx.zhimg.com/v2-f6d40edee59ca010ee1d75143754e52e_720w.jpg?source=172ae18b)](https://zhuanlan.zhihu.com/c_2076990667423859580)

[Go 1.27 context 三部曲](https://zhuanlan.zhihu.com/c_2076990667423859580)

[

炼石

2 篇内容 · 6 赞同

](https://zhuanlan.zhihu.com/c_2076990667423859580)

[

最热内容 ·

Go context（基于1.27新版） 工程实战：Cause 归因、request id 贯穿日志、优雅退出与外部进程

](https://zhuanlan.zhihu.com/c_2076990667423859580)

发布于 2026-08-28 18:44・浙江・包含 AI 辅助创作 作者对内容负责