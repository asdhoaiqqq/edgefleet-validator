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

### 分区休眠示例：等待、休眠与恢复

下面示例使用两个分区（`--partitions 2`）和 1000 毫秒固定窗口（`--window-ms 1000`），所有事件都属于同一个 key `k`。时间均为事件时间，单位毫秒，`time` 是非负有符号 64 位整数，`value` 是有符号 64 位整数，取值范围与上文记录格式的规定一致；整个过程完全离线，不读取系统时钟。命令、逐行输入和输出都可以直接复制运行：

```bash
printf '%s\n' \
  '{"type":"event","key":"k","time":100,"value":2,"partition":0}' \
  '{"type":"event","key":"k","time":800,"value":3,"partition":1}' \
  '{"type":"watermark","time":2000,"partition":0}' \
  '{"type":"idle","partition":1}' \
  '{"type":"watermark","time":2000,"partition":1}' \
  '{"type":"watermark","time":3000,"partition":0}' \
  '{"type":"event","key":"k","time":2500,"value":4,"partition":1}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --partitions 2
```

标准输出只有一行：

```json
{"key":"k","start":0,"end":1000,"count":2,"sum":5}
```

两个分区同一 key、同一窗口的事件被合并：窗口 `[0,1000)` 内有第 1、2 行各一个事件，所以 `count = 2`、`sum = 2 + 3 = 5`；输出只含 `key、start、end、count、sum`，不含分区。逐行看关键点：

1. 第 1、2 行是事件，此时还没有任何水位线，没有任何窗口可以关闭，因此没有新结果。
2. 第 3 行把分区 0 的水位线推进到 2000，但分区 1 尚未上报过水位线，整体水位线仍不存在，仍然没有新结果。这里没有输出不是因为窗口还没到关闭时刻（分区 0 的 2000 已经足以关闭 end=1000 的窗口），而是在**等待其他分区**：整体水位线是各分区水位线的最小值，缺一个分区就无法计算。若允许分区 0 单独推进，分区 1 之后上报的低水位线或它继续送来的 time ≤ 2000 的事件就会被误判为迟到而丢弃。
3. 第 4 行 `{"type":"idle","partition":1}` 显式声明分区 1 休眠。休眠只由输入声明，一段时间没有输入不会被当作自动休眠。声明生效后分区 1 退出整体水位线计算，剩余活动分区只有分区 0 且已上报，整体水位线成为 2000，`[0,1000)` 的 end=1000 ≤ 2000，于是**这条休眠记录本身就关闭了窗口**，输出上面那一行。休眠不是丢弃已有事件：分区 1 的 time=800 事件照常计入了 count 和 sum。
4. 第 5 行是分区 1 的水位线 2000，分区从休眠中恢复。恢复水位线必须不低于该分区自己的旧水位线，也不低于当前整体水位线；分区 1 此前没上报过水位线，只需满足后者，而 2000 恰好等于当前整体水位线 2000，边界取等，恢复成功。恢复后整体水位线重新按两个分区取最小值 min(2000, 2000) = 2000。已经关闭的 `[0,1000)` 不会再次输出，所以这一行没有新结果。
5. 第 6 行分区 0 推进到 3000，但已恢复的分区 1 重新参与最小值计算，整体水位线被分区 1 的 2000 拉住，仍为 2000，没有新结果。
6. 第 7 行分区 1 的 time=2500 事件不低于整体水位线 2000，是有效事件，计入窗口 `[2000,3000)`，但该窗口 end=3000 > 2000，暂不关闭。
7. 输入到此结束。结束输入**不会补发任何尚未关闭的窗口**：`[2000,3000)` 中已有一个 count=1、sum=4 的事件，却没有任何水位线达到 3000，因此它永远不会被这次运行输出。要拿到它，需要继续输入记录，例如再依次送入两个分区的 `{"type":"watermark","time":3000,...}`。

### 两种容易误用的情况

以下失败都会在标准错误报告**物理输入行号**和原因、以非零状态退出，并且不再处理该行之后的任何记录；此前已经写入标准输出的结果保留。空行同样计入行号。

**误用一：恢复水位线低于该分区旧水位线或当前整体水位线。** 恢复水位线必须同时不低于两者，低于任意一个都会失败：

```bash
printf '%s\n' \
  '{"type":"event","key":"k","time":100,"value":2,"partition":0}' \
  '{"type":"watermark","time":5000,"partition":0}' \
  '{"type":"watermark","time":2000,"partition":1}' \
  '{"type":"idle","partition":1}' \
  '' \
  '{"type":"watermark","time":1999,"partition":1}' \
  '{"type":"event","key":"k","time":6000,"value":9,"partition":0}' \
  '{"type":"watermark","time":9000,"partition":0}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --partitions 2
```

第 3 行两个分区都已上报，整体水位线为 2000，`[0,1000)` 已在第 3 行关闭；第 4 行休眠后整体水位线推进到 5000；第 5 行是空行（计入行号）；第 6 行试图用 1999 恢复，低于分区 1 自己的旧水位线 2000，标准错误为：

```text
aggregate: line 6: resume watermark 1999 for partition 1 is below its previous watermark 2000
```

把第 6 行换成 `{"type":"watermark","time":3000,"partition":1}` 则是另一种失败：3000 不低于自己的旧水位线 2000，却低于休眠期间被分区 0 推进到的当前整体水位线 5000，标准错误为：

```text
aggregate: line 6: resume watermark 3000 for partition 1 is below the current effective watermark 5000
```

两种情况下标准输出都只保留第 3 行已输出的 `{"key":"k","start":0,"end":1000,"count":1,"sum":2}`；第 7、8 行不再处理（不会读取，也不会改变任何状态），分区仍处于休眠状态。取等号不是失败：恢复水位线恰好等于旧水位线或当前整体水位线时可以成功，见上文示例第 5 行。

**误用二：休眠期间直接发送事件。** 休眠分区只能由下一条水位线记录恢复，事件不会隐式恢复它；即使事件时间已经低于整体水位线，也不会按普通迟到事件写一条跳过提示后继续，而是直接失败：

```bash
printf '%s\n' \
  '{"type":"event","key":"k","time":100,"value":2,"partition":0}' \
  '{"type":"watermark","time":5000,"partition":0}' \
  '{"type":"idle","partition":1}' \
  '' \
  '{"type":"event","key":"k","time":100,"value":3,"partition":1}' \
  '{"type":"event","key":"k","time":6000,"value":9,"partition":0}' \
  '{"type":"watermark","time":9000,"partition":0}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --partitions 2
```

第 3 行声明从未上报过水位线的分区 1 休眠，整体水位线成为 5000 并关闭 `[0,1000)`；第 4 行是空行；第 5 行给休眠中的分区 1 发送事件，虽然 time=100 已低于整体水位线 5000，标准输出里也**不会**出现迟到跳过提示，而是致命错误：

```text
aggregate: line 5: event for idle partition 1 is not allowed; send a watermark to resume it first
```

标准输出只保留此前的 `{"key":"k","start":0,"end":1000,"count":1,"sum":2}`，第 6、7 行不再处理，分区 1 仍需先发一条合格的水位线记录恢复后才能接收事件。

## 技术方向

validator, node-monitoring, device-fleet, p2p-network, telemetry, devnet

## 运行要求

Go 1.26，仅使用标准库，全部行为可在本机 CPU 上离线复现。
