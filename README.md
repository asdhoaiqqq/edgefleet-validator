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

## 示例：滑动窗口、事件重复计数与迟到跳过

下面是一个可直接离线运行的完整示例：窗口长度 1000 毫秒（`--window-ms 1000`）、滑动间隔 600 毫秒（`--slide-ms 600`），不使用分区，围绕单一水位线和同一个 key `sensor-a` 展开。所有时间均以毫秒计，`time` 为非负的 64 位有符号整数，`value` 为 64 位有符号整数。

窗口起点从时间零开始、按滑动间隔 600 递增，即 0、600、1200、1800……每个窗口长度均为 1000，区间左闭右开：

```
[0,1000)  [600,1600)  [1200,2200)  [1800,2800) ...
```

一条事件会计入**所有**包含其事件时间的窗口（在每个窗口中各计一次数、各加一次完整的值），所以窗口重叠时同一事件会出现在多个结果行中；窗口仍然只在水位线推进时关闭。

命令与完整的逐行 JSON 输入（共 7 行）：

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

终端中标准输出与标准错误按处理顺序交错出现（行尾注释标明各自属于哪个流），进程正常结束，退出码为 0：

```
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}             # 标准输出（第 3 行水位线触发）
line 4: late event time=999 below current watermark 1000, skipped    # 标准错误（第 4 行事件触发）
{"key":"sensor-a","start":600,"end":1600,"count":3,"sum":9}           # 标准输出（第 6 行水位线触发）
```

即分开看时，标准输出恰好为两行：

```json
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}
{"key":"sensor-a","start":600,"end":1600,"count":3,"sum":9}
```

标准错误恰好为一行：

```
line 4: late event time=999 below current watermark 1000, skipped
```

逐行说明（行号即物理输入行号）：

- 第 1 行：事件时间 700、值 2。因为 0 ≤ 700 < 1000 且 600 ≤ 700 < 1600，它**同时属于 `[0,1000)` 和 `[600,1600)` 两个窗口**，在两个窗口中各计一次 count、各加一次 2。这就是滑动窗口下同一事件会出现在多个结果中的原因。此时还没有任何水位线，没有输出。
- 第 2 行：事件时间 1000、值 3。区间左闭右开，时间恰好等于前一个窗口的右端点，所以 1000 **不属于 `[0,1000)`**；它属于 `[600,1600)` 和起点为 1000 的 `[1000,2000)`。此时仍没有水位线，没有输出。
- 第 3 行：水位线推进到 1000。所有 end ≤ 1000 的窗口关闭，符合条件的只有 `[0,1000)`（end 恰好等于 1000，取等关闭）；其中只有第 1 行的事件，因此输出 count=1、sum=2。`[600,1600)` 的 end 为 1600，大于水位线，仍然继续等待，这就是标准输出的第一行。
- 第 4 行：事件时间 999、值 9。999 虽然仍落在尚未关闭的重叠窗口 `[600,1600)` 之内，但迟到判断先于入窗：999 严格低于当前水位线 1000，事件被**整体跳过**，不进入任何窗口，标准错误按物理行号报告迟到原因（见上面的标准错误行）。它的值 9 不会贡献给任何计数或求和。
- 第 5 行：事件时间 1000、值 4。时间**恰好等于当前水位线**，规则只拒绝严格低于水位线的事件，所以这条仍被正常接收，进入 `[600,1600)` 和 `[1000,2000)`，没有输出。
- 第 6 行：水位线推进到 1600，关闭 `[600,1600)`（已关闭的 `[0,1000)` 不会重复输出，`[1000,2000)` 的 end 为 2000，仍要等待）。这个窗口里是第 1、2、5 行的三条事件（时间 700/值 2、1000/值 3、1000/值 4），所以 count=3、sum=2+3+4=9；第 4 行被跳过的事件不在其中。这就是标准输出的第二行。
- 第 7 行：事件时间 1700、值 5。1700 不低于当前水位线 1600，是合法事件，被收入尚未关闭的窗口；随后输入正常结束。**结束输入不会补发任何仍未关闭的窗口**，所以这条事件所在的窗口不产生任何输出。第 4 行的迟到提示只是标准错误上的一条说明，并不是运行失败：整个进程退出码仍为 0。

省略 `--slide-ms`，或令 `--slide-ms` 等于 `--window-ms`（例如两者都是 1000）时，窗口退化为互不相交的 `[0,1000)`、`[1000,2000)`……与原有固定窗口功能完全一致。滑动间隔**无需整除**窗口长度——本例中 600 并不整除 1000，窗口划分与计数照常工作——但它必须是正整数，并且不大于窗口长度。

### 误用：滑动间隔大于窗口长度

间隔超过窗口长度会在启动时直接失败，发生在读取任何输入之前：参数校验先于输入处理，所以管道里的事件一行都不会被读取，也不会有窗口结果或迟到提示。用构建出的二进制可以直接看到程序自身的退出码 2：

```bash
go build -o /tmp/edgefleet ./cmd/edgefleet
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":700,"value":2}' \
  | /tmp/edgefleet aggregate --window-ms 1000 --slide-ms 1200
echo "exit=$?"
```

标准输出为空，标准错误为：

```
aggregate --slide-ms 1200 must not exceed --window-ms 1000
```

```
exit=2
```

若改用 `go run ./cmd/edgefleet ...` 运行，`go run` 会把程序的非零退出统一报成退出码 1，并在标准错误多打印一行 `exit status 2`；被包装的程序自身退出码仍是 2，如上构建二进制后即可直接观察到。

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
