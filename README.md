# 验证者与边缘节点机群管理平台

## 用途

节点注册与身份、心跳与遥测、离线同步、配置分发与滚动升级、罚没与健康规则告警、远程运维记录。

本仓库是可持续演进的自托管 Go 应用。领域核心位于 `edgefleet/`，命令入口位于 `cmd/edgefleet/`。

```bash
go run ./cmd/edgefleet demo
go run ./cmd/edgefleet version
go test ./...
```

## 心跳与遥测

平台通过独立命令接收、保存和查询真实心跳，节点断连期间缓存的心跳可在恢复后补传。数据持久保存在本机，平台重启后仍能识别已接收的数据。

```bash
# 提交心跳（从 stdin 读取 JSON 数组）
go run ./cmd/edgefleet heartbeat submit --receive-time 2026-10-01T12:00:00+08:00 <<'EOF'
[{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":12345,"missed":0}]
EOF

# 查询健康（基于最新遥测，60 秒内在线）
go run ./cmd/edgefleet heartbeat health --node val-eu-1 --expected-version 1.26.0 --tolerated-misses 0

# 以一条历史心跳为基准，只统计其后新增的漏签（严格大于容忍值才告警）
go run ./cmd/edgefleet heartbeat health --node val-eu-1 --expected-version 1.26.0 \
  --tolerated-misses 2 --missed-since-seq 7

# 查询全部历史心跳（按序号升序，不重复）
go run ./cmd/edgefleet heartbeat history --node val-eu-1

# 完整帮助（含输入格式与数据保存位置）
go run ./cmd/edgefleet heartbeat --help
```

每条心跳包含节点标识、大于 0 的整数序号、采集时间（带时区）、版本、区块高度和累计漏签数，所有字段均需明确提供。数据默认保存在 `~/.edgefleet`（可用 `--data-dir` 或 `EDGEFLEET_DATA_DIR` 覆盖），按节点分文件存储，写入原子化并对目录加锁，支持多进程并发访问；数据损坏时明确拒绝读取和继续写入。

节点标识和版本可以是任意合法 Unicode 文本（中文、表情、空格、换行均可提交、保存和查询）。健康查询输出时，不含空白、双引号、反斜杠或控制字符的文本按原样显示；其余文本显示为带双引号的 JSON 字符串（换行、回车、制表符显示为 `\n`、`\r`、`\t`，其他控制字符显示为 `\uXXXX`），因此每条健康结果始终只占一行，带引号的显示值按 JSON 字符串解读可还原原文。这只是展示规则：节点身份和版本比较仍使用原始文本，存储内容不会被改写。

### 健康查询如何选取记录与判断在线

健康查询始终使用该节点已保存记录中**序号最大**的一条作为“最新记录”，输出中的 `seq`、`collected_at`、`version`、`height`、`missed`（累计漏签数）全部来自这一条。采集时间更晚或接收时间更晚的较小序号记录不会替代它，也不会与它拼接字段。

因此，节点恢复连接并补传断连期间缓存的心跳后，**补传成功仍可能显示离线**：补传成功（`submitted: new=1`）只表示这条历史记录已被接收并保存，不代表节点重新在线。若补传记录的序号小于已保存的最大序号，健康查询根本不会选中它；即使它的采集时间很新，也不会把节点“刷新”为在线。

在线状态看的是**选中记录的采集时间到查询时间的间隔**：间隔不超过 60 秒为在线（恰好 60 秒仍为在线），超过 60 秒即为离线。`--at` 指定本次判断使用的查询时间，省略时使用当前时间。比较使用真实时刻（纳秒精度）：带小数秒的时间不会先取整再比较；同一时刻的不同时区写法（如 `2026-10-01T12:01:00.5Z` 与 `2026-10-01T20:01:00.5+08:00`）代表同一时刻，结论一致。

注意终端输出的 `collected_at` 只显示到整秒（RFC3339 不含小数部分），而存储和判断保留完整精度。两条结果可能打印出相同的整秒采集时间，却一个在线一个离线——不要从打印的时间推导边界结论，以查询命令给出的 `status` 为准。

接收时间（`--receive-time`，省略时为提交时刻）只在提交时参与时间合法性判断：采集时间晚于接收时间的记录会被拒绝。它不被保存，也不参与健康查询；**不能把补传的接收时间当作节点最近的活动时间**。

`--at` 早于最大序号记录的采集时间时，命令明确报错（`error: query time ... is earlier than collection time ...`）并以非零状态退出——即使另有一条较小序号记录早于查询时间，也不会改用它。而正常查询得到离线状态仍是成功返回，退出状态为 0。

`--missed-since-seq` 基准只影响漏签告警的统计口径（见下节），不改变所选记录，也不改变在线状态的判断。

### 本机离线示例（补传后仍显示离线）

以下示例使用固定时间，结果不随执行时刻变化，可照此复现。节点 `val-eu-1` 断连前已上报 seq 8（采集于 11:58:00）；恢复连接后补传了断连期间缓存的 seq 5（采集于 11:59:50，更晚但序号更小）。

```bash
# 1. 断连期间，平台先收到较大序号的记录（采集时间更早）
go run ./cmd/edgefleet heartbeat submit --data-dir /tmp/hb-demo --receive-time 2026-10-01T11:58:30Z <<'EOF'
[{"node":"val-eu-1","seq":8,"collected_at":"2026-10-01T11:58:00Z","version":"1.25.0","height":1000,"missed":5}]
EOF
# 输出: submitted: new=1 duplicate=0

# 2. 节点恢复连接，补传较小序号的历史记录（采集时间更晚）
go run ./cmd/edgefleet heartbeat submit --data-dir /tmp/hb-demo --receive-time 2026-10-01T12:02:00Z <<'EOF'
[{"node":"val-eu-1","seq":5,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":900,"missed":2}]
EOF
# 输出: submitted: new=1 duplicate=0   —— 补传成功，仅表示历史已接收

# 3. 历史保留两条记录，按序号升序
go run ./cmd/edgefleet heartbeat history --data-dir /tmp/hb-demo --node val-eu-1
# node=val-eu-1 seq=5 collected_at=2026-10-01T11:59:50Z version=1.26.0 height=900 missed=2
# node=val-eu-1 seq=8 collected_at=2026-10-01T11:58:00Z version=1.25.0 height=1000 missed=5

# 4. 健康查询仍取序号最大的 seq 8：到 12:00:00 已间隔 120 秒，离线
go run ./cmd/edgefleet heartbeat health --data-dir /tmp/hb-demo --node val-eu-1 \
  --expected-version 1.26.0 --tolerated-misses 4 --at 2026-10-01T12:00:00Z
# node=val-eu-1 status=offline seq=8 collected_at=2026-10-01T11:58:00Z version=1.25.0 height=1000 missed=5 findings=[offline version skew: 1.25.0 != 1.26.0 missed duties above tolerance]
# 退出状态为 0（离线是正常查询结果）
```

seq 5 的记录采集时间更新、版本也符合预期，但健康结果完全不使用它：版本、高度、累计漏签数与在线状态都来自 seq 8。

下面的示例用带小数秒的采集时间展示 60 秒边界两侧的结果，以及“显示到整秒”与“按真实时刻判断”的区别：

```bash
go run ./cmd/edgefleet heartbeat submit --data-dir /tmp/hb-demo --receive-time 2026-10-01T12:02:00Z <<'EOF'
[{"node":"val-ap-1","seq":1,"collected_at":"2026-10-01T12:00:00.5Z","version":"1.26.0","height":100,"missed":0}]
EOF

# 恰好 60 秒：在线（--at 与采集时间都带 0.5 秒小数）
go run ./cmd/edgefleet heartbeat health --data-dir /tmp/hb-demo --node val-ap-1 \
  --expected-version 1.26.0 --tolerated-misses 0 --at 2026-10-01T12:01:00.5Z
# node=val-ap-1 status=online seq=1 collected_at=2026-10-01T12:00:00Z version=1.26.0 height=100 missed=0 findings=[]

# 60 秒零 1 纳秒：离线，仍是成功返回（退出状态 0）
go run ./cmd/edgefleet heartbeat health --data-dir /tmp/hb-demo --node val-ap-1 \
  --expected-version 1.26.0 --tolerated-misses 0 --at 2026-10-01T12:01:00.500000001Z
# node=val-ap-1 status=offline seq=1 collected_at=2026-10-01T12:00:00Z version=1.26.0 height=100 missed=0 findings=[offline]
```

两次查询打印的 `collected_at` 完全相同（整秒显示抹去了 `.5`），结论却相反——判断用的是未取整的真实时刻。把 `--at` 写成同一时刻的其他时区形式（如 `2026-10-01T20:01:00.5+08:00`）结论不变。

若 `--at` 早于最大序号记录的采集时间，命令报错并以非零状态退出，不会回退到较小的序号：

```bash
go run ./cmd/edgefleet heartbeat health --data-dir /tmp/hb-demo --node val-eu-1 \
  --expected-version 1.26.0 --tolerated-misses 4 --at 2026-10-01T11:57:30Z
# error: query time 2026-10-01T11:57:30Z is earlier than collection time 2026-10-01T11:58:00Z for node "val-eu-1"
# 退出状态为 1；命令不会为了给出结果而换用其他序号的记录
```

### 以历史心跳为基准的漏签查询

不带 `--missed-since-seq` 时，漏签告警基于最新心跳的累计漏签数。加上该参数（必须是该节点已保存的一条心跳序号，大于 0）后，最新心跳仍取序号最大的一条，漏签告警改为比较“从基准心跳到最新心跳之间新增的漏签数”（最新累计数减去基准累计数），只有严格大于 `--tolerated-misses` 才告警，等于不告警；输出首行的 `missed` 仍为累计数，第二行明确给出 `baseline_seq`、`baseline_missed`、`new_missed` 与 `tolerated_misses`。基准就是最新记录时新增数为 0。基准只影响本次查询，不改写历史，也不影响后续不带参数的判断。

序号允许有间隔。若基准至最新之间任意两条按序号相邻的已保存记录出现累计值下降（例如节点重置计数器），查询照常返回，但 `new_missed` 显示为“无法判断”，findings 中给出“累计漏签数回退”，且不再产生漏签超限告警——即使最新累计数后来重新超过基准也一样；基准之前的下降不影响本次判断。事后补入区间的历史记录若揭示了回退，之后的查询会反映这一变化；重复提交同一记录不会重复计算。节点无遥测、基准序号不存在或大于最新序号时，命令明确报错并以非零状态退出，不会换用其他记录或把基准累计数当作 0。在线状态仍按最新采集时间判断，版本偏移照常报告，均不受基准影响。

## 技术方向

validator, node-monitoring, device-fleet, p2p-network, telemetry, devnet

## 运行要求

Go 1.26，仅使用标准库，全部行为可在本机 CPU 上离线复现。
