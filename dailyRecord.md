# lab1

### 2022.10.20

解决 map 输出临时文件，reduce 读取问题。用推荐的 json 存储

今天引入进程 pid 用作 server 端记录任务分配给哪个进程，所以任务状态映射的值变化为：-1 -> 未进行的任务，-2 -> 已完成的任务，大于 0 的值 -> 正在等待 worker 反馈的值 

### 2022.10.21

解决 import cycle 的问题。修改小 bug

### 2022.10.22 

解决中间文件的问题，通过 crash 之外的其他测试。明天需要解决，worker 超时回收任务再分配

加油最后的一个问题！

### 2022.10.23

今天实验室服务器崩了，后续得用 wsl 来写

### 2022.10.24

**PASSED ALL TESTS** lab 1 完结撒花！！！

### 2022.12.6

**PASSED ALL TESTS** 突然发现 lab1 存在几个 bug

+   临时文件写入磁盘应该由 master 完成不能给 worker 完成。思考如下过程：分配的 worker 速度慢了，master 认为它挂了于是再分配新的 worker 去工作，但是之前的 worker 并没有挂，这时候怎么保证两个 worker 不冲突。这时候就需要 commit 给 master 的时候由 master 将临时文件写入磁盘。 不然两个 worker 就冲突了。
+   由上述问题引出的问题，判断当前任务完成需要完成任务 worker 的 pid 与任务分配的 worker 的 pid 相同才能算任务完成。二者 pid 不同就说明，存在了上面的问题，只信当前任务分配的 worker 进程的结果，其他的都不信。其他的 worker 由于此时只有任务完成的临时文件，没有写入磁盘，所以直接丢弃这些文件。

# lab2A

### 2022.12.16

读了系统框架，这个lab大概的意思读明白了。 Raft 到底是什么有了新的认知。
剩余问题：Heartbeat，AppendEntry 在哪里怎么实现

### 2022.12.17

今天看懂了 Heartbeat 和 AppendEntry 的问题，问就是看 lab 的 hint。对 go 的并发还是了解太少导致有时候有思路怎么写，但是不知道用 go 怎么实现。
剩下的问题：状态机的控制，还有一些细节，明天需要仔细检查一下整个逻辑。best wishes！

### 2022.12.18

今天完成了第一个测试点，接下来是网络错误测试和多 Raft 测试，还有 bug 没过，明天看一看
剩下的问题：选举出错 or 其他问题的恢复

### 2022.12.19

遇到一个问题，leader 发心跳的时候网络故障发慢了。
+   这时候其他人当leader发来心跳应该怎么办？回到 fellower
+   如果是candidate选举的时候收到前面发慢的心跳怎么办？旧心跳 term 小于现在 term，直接返回false，然后election 里面就变回 fellower。
今天改了很多bug，现在应该还剩下一点点。今天状态不好，明天再战！明天必完成！

### 2022.12.20

终于发现了bug，草！原来是ch定义，如果 `make(ch, num)` 不加 num，就是非缓冲的ch，收发数据的两端必须同时准备好才能写入ch数据。我的程序里面，一个 server 恢复之后，因为收到 heartbeat 的时候锁了 server 的raft结构，这就导致 ticker 协程没办法获取 rf.mu ，就导致接收端还没准别好，就导致写入 ch 一直等待，就死了。

### 2022.12.24

今天完成了 lab2A，千里之行始于足下！发现了前几天的bug，就是在 ticker 的理解上，前面理解错了，导致一直 bug。今天终于想明白了。完结撒花

# lab2B

### 2022.12.24

刚开始一点点，逻辑还没理顺。建议先看论文在写！

### 2022.12.25

md 一个任期只能投给一个leader票，votefor不能因为收到心跳就变成-1

### 2022.12.31

今天新冠应该是康复了很多。乱七八糟的事情也结束了七七八八，接下来目标两个周结束raft。给2023开个好头。

### 2023.1.1

剩下的部分：lastapplied 和 commitindex 两个部分没太明白，得看看论文

### 2023.2.4

student guidence 大赞。解决了两个问题：
1. heartbeat 和 appendentry 是一个东西，只是 heartbeat 里面内容是空的，所以需要的收到 appendentry rpc 中即使是心跳也需要检查条件。
2. apply 用一个协程一直监控或者 commitindex 改变的时候立马检查

### 2023.2.5

基本写完了代码，接下来要debug

### 2023.2.7

今天的改动主要有两个：
1. appendentry 失败的时候直接next--，然后等下一次appendentry再试试就行了，不能原地立马重试
2. 更新 commitindex 要在半数 appendentry 成功之后马上更新

test 剩下 TestBackup2B 一个没过，明天要去北京，争取明早搞定它，不知道有空没有。龙颜大悦！！！

### 2023.2.8

剩下一个 bug，选举成功以后大约5s会突然选举失败然后重新选举，很离谱，不知道是 test 代码的问题还是我的代码的问题，大概率是我的问题。今天没时间了，等回来再瞧瞧！有一定几率不能复现

剩下的问题是，恢复的过程太慢了导致超时

### 2023.2.13

今日北京归来！成功debug，成为leader之后，votefor不能改1。优化了xlen，xindex和xterm，没优化过不了，而且现在还是时间很久，可能会是个大坑！！！

### 2023.2.16

2c有bug，我重新看一下student guide。有以下几个问题未理解：
1. If a step says “reply false”, this means you should reply immediately, and not perform any of the subsequent steps. 这里如果直接 reply ，有一个问题是 figure2 里面第 3 点讲了 If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it. 这里没太明白！
Another issue many had (often immediately after fixing the issue above), was that, upon receiving a heartbeat, they would truncate the follower’s log following prevLogIndex, and then append any entries included in the AppendEntries arguments. This is also not correct. We can once again turn to Figure 2:
The if here is crucial. If the follower has all the entries the leader sent, the follower MUST NOT truncate its log. Any elements following the entries sent by the leader MUST be kept. This is because we could be receiving an outdated AppendEntries RPC from the leader, and truncating the log would mean “taking back” entries that we may have already told the leader that we have in our log.

事实证明不能脑补，直接按照论文一字不差的做事能过的！2C 完成！

今天开始看了一下 lab 2D，怎么感觉又是大工程！晕不想看 Raft 了，今天学别的去了！

### 2023.2.17

今日整了个 obsiden 的 github 主题顺眼多了。

总算是看明白 lab2D 和整个 Raft 的架构。对于 Raft 和整个架构有更深刻的理解。这里面有三个重要的概念：
1. client 用户：这是用户发起请求的地方，根据应用程序的不同，请求不同
2. service 节点 server 服务器：这是服务器是处理用户请求的地方。一个分布式集群拥有很多台物理服务器，做物理复制（一种是 state transfer，一种是物理复制）。这台服务器根据应用的不同运行着不同的服务程序，为了保持每个物理服务器的一致性就有了 Raft 这样的一致性算法。 Raft 同样运行在 server 上，用来保持服务器集群的一致性。Raft 只是一个程序，或者说是服务器上运行的一个服务。

快照由两部分组成，一部分是服务器上应用程序的快照（比如数据库应用的键值对），另一部分是 Raft 层中日志 log 的 lastIndex 和 lastTerm，所以 snapshot 由 service 生成，发送给 Raft 去删除冗余日志。当日志数量高到一定程度，service 生成 snapshot 告诉 Raft 节点删除那个 index 之前的日志，保存一下我的 snapshot 就可以。每个 Raft 节点有需要保存自己物理服务器的 snapshot，因为自己可能成为 leader，成为 leader 发现有 fellower 的 log 太慢了，这时候可以不用 appendentry 一次次试着恢复，直接用 rpc（installsnapshot）把 leader 服务器的日志给他，让他发给自己的服务器。因为 snapshot 就是执行完后来的日志后 service 的样子，所以相当于 fellow 的 log 太慢了，不用你 append 之后 apply 给 fellow 的服务器执行，leader 直接告诉你执行后服务器的状态，你改成那个状态就可以。

最后一个问题，就是 fellower 收到 leader 的 snapshot 之后为什么要先给自己物理服务器发一份，等物理服务器保存后，物理服务器调用  condinstallsnapshot 之后 Raft 再保存 snapshot 删除冗余日志。这里是异步的？思考一下

### 2023.2.19

今天是开学前一天，做了 2D 的实验。貌似是完成了，完整的测试在跑，说不定有什么bug。

今天发现一个 go 语言的问题。`a := make([]*Entry,1)` 之后 `a = append(a, &Entry{})`，0 下标访问第一个元素出错，通过测试是在下标 1 的位置。剩下的 bug 都是在索引系统重置之后的一些没修改的！

对于 Raft 的两个疑问
1. Figure8 里面为什么 leader 更新 commitIndex 的时候需要多数 matchindex 
2. condinstallsnapshot 和 installsnapshot rpc 为什么异步

有个问题是性能有点差，特别是 installsnapshot 测试耗时很久，能过但是时间有点长。感觉是锁的问题？还是什么的？代码逻辑没有问题