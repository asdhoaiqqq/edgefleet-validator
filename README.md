# 验证者与边缘节点机群管理平台

## 用途

节点注册与身份、心跳与遥测、离线同步、配置分发与滚动升级、罚没与健康规则告警、远程运维记录。

本仓库是可持续演进的自托管 Go 应用。领域核心位于 `edgefleet/`，命令入口位于 `cmd/edgefleet/`。

```bash
go run ./cmd/edgefleet demo
go run ./cmd/edgefleet version
go run ./cmd/edgefleet help
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":1200,"value":5}' \
  '{"type":"watermark","time":2000}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000
go test ./...
```

`aggregate` 从标准输入读取逐行 JSON 事件与水位线记录，按事件时间汇总固定长度窗口（左闭右开，从时间零开始），只在输入水位线推进时输出 end ≤ 水位线的窗口；完全离线，不使用当前时间。可选的 `--slide-ms <milliseconds>` 给出相邻窗口起点的间隔（正数、不大于 `--window-ms`、不要求整除），起点依次为 0、一个间隔、两个间隔，事件会计入所有包含其事件时间的重叠窗口；省略该参数或令其等于窗口长度时与固定窗口行为完全一致。`--partitions <count>` 可合并同一输入流的多个分区：各分区水位线独立推进，全部到齐后整体水位线取各分区最小值，分区字段不进入输出，迟到判断与窗口关闭均使用该整体水位线。分区模式下可用 `{"type":"idle","partition":N}` 显式声明某分区暂时无数据，休眠只由输入声明、不依据时间推断；休眠分区退出整体水位线计算，待下一条不低于其旧水位线和当前整体水位线的水位线记录恢复。详见 `go run ./cmd/edgefleet help`。

## 示例：滑动窗口与迟到事件

下面是一个可直接离线运行的完整示例：单一水位线（不带 `--partitions`）、同一个 key `sensor-a`、1000 毫秒窗口长度（`--window-ms 1000`）与 600 毫秒滑动间隔（`--slide-ms 600`）。窗口起点从零开始、按 600 递增：依次为 `[0,1000)`、`[600,1600)`、`[1200,2200)`……每个区间左闭右开，因此一个事件会同时计入所有包含其事件时间的重叠窗口。

命令：

```bash
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":700,"value":2}' \
  '{"type":"event","key":"sensor-a","time":1000,"value":3}' \
  '{"type":"watermark","time":1000}' \
  '{"type":"event","key":"sensor-a","time":999,"value":9}' \
  '{"type":"event","key":"sensor-a","time":1000,"value":4}' \
  '{"type":"watermark","time":1600}' \
  '{"type":"event","key":"sensor-a","time":1700,"value":5}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --slide-ms 600
```

完整的标准输出：

```json
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}
{"key":"sensor-a","start":600,"end":1600,"count":3,"sum":9}
```

完整的标准错误（迟到提示，不致命）：

```
line 4: late event time=999 below current watermark 1000, skipped
```

进程正常结束，退出码为 0。逐行说明（行号即物理输入行号）：

- 第 1 行：事件时间 700 同时落在 `[0,1000)` 和 `[600,1600)` 两个窗口内，各计一次 count、各加一次 value=2。这就是滑动窗口与固定窗口的区别：同一事件会出现在多个结果中。此时没有水位线，无输出。
- 第 2 行：事件时间 1000 恰好是 `[0,1000)` 的右端点，区间左闭右开，所以它不属于 `[0,1000)`，只计入 `[600,1600)` 和 `[1200,2200)`。
- 第 3 行：水位线推进到 1000，关闭所有 end ≤ 1000 的窗口。满足条件的只有 `[0,1000)`，输出第一行结果（count=1、sum=2——只含第 1 行那一份）。`[600,1600)` 的 end 是 1600，大于水位线，继续等待。
- 第 4 行：事件时间 999 虽然仍落在尚未关闭的重叠窗口 `[600,1600)` 内，但 999 低于当前水位线 1000，整条事件被跳过，不计入任何窗口。标准错误报告物理行号和迟到原因：`line 4: late event time=999 below current watermark 1000, skipped`。迟到只是跳过该事件，不是错误，处理继续。
- 第 5 行：事件时间 1000 恰好等于当前水位线，仍被接收（水位线判断是"低于"才迟到），计入 `[600,1600)` 和 `[1200,2200)`。
- 第 6 行：水位线推进到 1600，关闭 `[600,1600)`，输出第二行结果：count=3、sum=2+3+4=9，分别来自第 1、2、5 行；第 4 行被跳过的事件不贡献计数和求和。`[1200,2200)` 仍未关闭。
- 第 7 行：事件时间 1700 是合法事件（不低于水位线 1600），计入 `[1200,2200)` 和 `[1800,2800)`。此后输入正常结束；结束输入不会补发尚未关闭的窗口，所以该事件不产生任何输出，迟到提示也不影响结果，退出码仍为 0。

省略 `--slide-ms` 或令其等于 `--window-ms` 时，行为与原有固定窗口完全一致；滑动间隔无需整除窗口长度，但必须为正数且不大于窗口长度。

### 误用：滑动间隔超过窗口长度

`--slide-ms` 大于 `--window-ms` 是启动参数错误，在读取任何输入之前就会失败。下面标准输入里明明有一行合法事件，但它根本不会被读取：

```bash
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":700,"value":2}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --slide-ms 1200
```

标准输出为空，标准错误给出原因，退出码为 2：

```
aggregate --slide-ms 1200 must not exceed --window-ms 1000
```

（用 `go run` 观察时，Go 工具会把它转述为 `exit status 2` 并以 1 退出；直接运行编译后的二进制即可看到退出码 2。）

## 示例：分区休眠与恢复

下面是一个可直接离线运行的完整示例：两个分区（`--partitions 2`）、1000 毫秒固定窗口（`--window-ms 1000`），围绕同一个 key `sensor-a` 展示从等待到休眠再到恢复的全过程。所有时间均以毫秒计，`time` 为非负的 64 位有符号整数，`value` 为 64 位有符号整数。

命令：

```bash
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":100,"value":5,"partition":0}' \
  '{"type":"event","key":"sensor-a","time":200,"value":3,"partition":1}' \
  '{"type":"watermark","time":1000,"partition":0}' \
  '{"type":"idle","partition":1}' \
  '{"type":"event","key":"sensor-a","time":1100,"value":7,"partition":0}' \
  '{"type":"watermark","time":2000,"partition":0}' \
  '{"type":"watermark","time":2000,"partition":1}' \
  '{"type":"event","key":"sensor-a","time":2100,"value":4,"partition":1}' \
  '{"type":"watermark","time":3000,"partition":0}' \
  '{"type":"watermark","time":3000,"partition":1}' \
  '{"type":"event","key":"sensor-a","time":3100,"value":9,"partition":0}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --partitions 2
```

完整的标准输出（进程退出码为 0）：

```json
{"key":"sensor-a","start":0,"end":1000,"count":2,"sum":8}
{"key":"sensor-a","start":1000,"end":2000,"count":1,"sum":7}
{"key":"sensor-a","start":2000,"end":3000,"count":1,"sum":4}
```

两个分区的事件合并进同一个窗口，输出只含 `key`、`start`、`end`、`count`、`sum`，不含分区字段，因此可以直接从输入算出每个结果。逐行说明（行号即物理输入行号）：

- 第 1、2 行：两个分区各来一个事件，事件时间 100 和 200 都落在窗口 `[0,1000)` 内，合并累计 count=2、sum=5+3=8。此时没有任何输出——还没有水位线。
- 第 3 行：分区 0 的水位线推进到 1000，但分区 1 从未上报过水位线，整体水位线尚不存在（不是取已上报分区的值），所以窗口 `[0,1000)` 不关闭，没有新结果。这就是"等待"：一个分区已经推进，另一个分区尚未上报时，只能等后者表态，否则无法判断它是否还有更早的事件在路上。
- 第 4 行：分区 1 显式声明休眠。休眠分区退出整体水位线的最小值计算，剩下的活跃分区只有分区 0，整体水位线立即变为 1000，于是这条休眠记录本身就关闭了窗口 `[0,1000)`，输出第一行结果。可见休眠不是"丢弃"该分区：它已贡献的事件（第 2 行）照常计入，只是不再拖住其他分区。
- 第 5 行：事件落入窗口 `[1000,2000)`，无输出。
- 第 6 行：分区 0 水位线推进到 2000；分区 1 仍在休眠、不参与计算，整体水位线变为 2000，关闭 `[1000,2000)`，输出第二行结果（count=1、sum=7）。
- 第 7 行：分区 1 用水位线记录恢复。恢复水位线必须不低于该分区自己的旧水位线、也不低于当前整体水位线；这里 2000 恰好等于当前整体水位线 2000，取等成功。恢复后整体水位线为 min(2000, 2000) = 2000，没有新窗口可关——已关闭的窗口不会再次输出，所以这一行没有新结果。
- 第 8 行：分区 1 恢复后重新发送事件，落入窗口 `[2000,3000)`，无输出。
- 第 9 行：分区 0 水位线推进到 3000，但恢复后的分区 1 重新参与整体水位线的最小值计算，整体水位线为 min(3000, 2000) = 2000，窗口 `[2000,3000)` 不能关闭，没有新结果。
- 第 10 行：分区 1 水位线推进到 3000，整体水位线变为 3000，关闭 `[2000,3000)`，输出第三行结果（count=1、sum=4）。
- 第 11 行：事件落入窗口 `[3000,4000)`，此后输入结束。结束输入不会补发尚未关闭的窗口——窗口只由水位线（含休眠声明带来的水位线推进）关闭，所以该事件不产生任何输出。

注意：休眠必须由输入用 `{"type":"idle","partition":N}` 显式声明，一段时间没有输入不会被当成自动休眠；休眠也不是无限水位线，不会丢弃该分区已有或待处理的事件。

### 误用一：恢复水位线过低

休眠分区的恢复水位线若低于当前整体水位线，或低于该分区自己休眠前的旧水位线，都会失败。例如整体水位线已到 2000 时用 1500 恢复：

```bash
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":100,"value":5,"partition":0}' \
  '{"type":"watermark","time":1000,"partition":0}' \
  '{"type":"idle","partition":1}' \
  '{"type":"watermark","time":2000,"partition":0}' \
  '{"type":"watermark","time":1500,"partition":1}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --partitions 2
```

标准输出保留失败前已关闭的窗口，标准错误报告物理行号和原因，退出码为 1：

```
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":5}   # 标准输出
aggregate: line 5: resume watermark 1500 for partition 1 is below the current effective watermark 2000   # 标准错误
```

若该分区休眠前已上报过更高的水位线，则报另一种原因，例如旧水位线为 2500 时用 2400 恢复：

```
aggregate: line 4: resume watermark 2400 for partition 1 is below its previous watermark 2500
```

### 误用二：休眠期间直接发送事件

休眠分区只能用下一条水位线记录恢复，不能直接发事件。下面第 5 行的事件时间 500 虽然已经低于当前整体水位线 1000，但不会按普通迟到事件跳过并记一条迟到日志，而是致命错误（注意第 4 行是空行，空行也计入行号，所以出错的是第 5 行）：

```bash
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":100,"value":5,"partition":0}' \
  '{"type":"watermark","time":1000,"partition":0}' \
  '{"type":"idle","partition":1}' \
  '' \
  '{"type":"event","key":"sensor-a","time":500,"value":1,"partition":1}' \
  '{"type":"watermark","time":2000,"partition":0}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --partitions 2
```

```
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":5}   # 标准输出
aggregate: line 5: event for idle partition 1 is not allowed; send a watermark to resume it first   # 标准错误
```

这两类失败都会报告物理输入行号和原因、停止处理后续记录（上例中第 6 行的水位线不会再被读取），退出码为 1，但此前已输出的窗口结果全部保留。

## 技术方向

validator, node-monitoring, device-fleet, p2p-network, telemetry, devnet

## 运行要求

Go 1.26，仅使用标准库，全部行为可在本机 CPU 上离线复现。
