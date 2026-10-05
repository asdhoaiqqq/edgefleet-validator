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

### 以历史心跳为基准的漏签查询

不带 `--missed-since-seq` 时，漏签告警基于最新心跳的累计漏签数。加上该参数（必须是该节点已保存的一条心跳序号，大于 0）后，最新心跳仍取序号最大的一条，漏签告警改为比较“从基准心跳到最新心跳之间新增的漏签数”（最新累计数减去基准累计数），只有严格大于 `--tolerated-misses` 才告警，等于不告警；输出首行的 `missed` 仍为累计数，第二行明确给出 `baseline_seq`、`baseline_missed`、`new_missed` 与 `tolerated_misses`。基准就是最新记录时新增数为 0。基准只影响本次查询，不改写历史，也不影响后续不带参数的判断。

序号允许有间隔。若基准至最新之间任意两条按序号相邻的已保存记录出现累计值下降（例如节点重置计数器），查询照常返回，但 `new_missed` 显示为“无法判断”，findings 中给出“累计漏签数回退”，且不再产生漏签超限告警——即使最新累计数后来重新超过基准也一样；基准之前的下降不影响本次判断。事后补入区间的历史记录若揭示了回退，之后的查询会反映这一变化；重复提交同一记录不会重复计算。节点无遥测、基准序号不存在或大于最新序号时，命令明确报错并以非零状态退出，不会换用其他记录或把基准累计数当作 0。在线状态仍按最新采集时间判断，版本偏移照常报告，均不受基准影响。

### 健康查询选哪条记录、为什么补传成功后仍可能离线

健康查询所说的“最新记录”只有一个含义：**该节点已保存记录中序号（`seq`）最大的那一条**。它与记录什么时候被平台接收、采集时间是早是晚都没有关系：

- 输出首行的 `seq`、`collected_at`、`version`、`height`、`missed` 全部来自这一条记录，不会从多条记录里各取一个字段拼接；`offline`、版本偏移、漏签超限等 findings 也都基于它。
- 采集时间更晚、但序号更小的记录不会替代它；接收时间更晚才补传到的较小序号记录（包括同一条记录的重复提交）也不会替代它。“最新”既不是“最后收到”，也不是“采集时间最晚”。

要区分三个时间：

- **采集时间**（心跳输入里的 `collected_at`）：节点在本机产生这条遥测的时刻，随记录持久保存，是在线判断唯一使用的活动时间。
- **接收时间**（提交时的 `--receive-time`）：平台收下这批数据的时刻，只参与提交时的合法性判断——采集时间晚于接收时间的记录会被拒绝。它不随记录保存，不表示节点最近活动，也不参与健康查询。补传发生得再晚，也不会把节点的活动时间“刷新”到补传时刻。
- **查询时间**（健康查询的 `--at`）：本次判断所站的时刻；省略 `--at` 时取当前时间。

因此，较小序号心跳在节点恢复连接后补传成功（`new=1`），只表示那条历史记录已被接收并保存，**不表示节点重新在线**。在线状态看的是“选中记录的采集时间 → 查询时间”的间隔，与接收时间无关：间隔不超过 60 秒为在线，**恰好 60 秒仍为在线，超过 60 秒即为离线**。节点要重新显示在线，需要再收到一条序号更大、且采集时间距查询时刻不超过 60 秒的新心跳。

比较使用的是带纳秒精度的真实时刻：采集时间和 `--at` 都可以带小数秒（如 `2026-10-01T12:00:00.5Z`），比较前不会先取整到整秒；同一时刻用不同时区写法表示（如 `12:01:00.5Z` 与 `20:01:00.5+08:00`）结论完全一致。注意终端里 `collected_at`（健康结果和 `history` 都一样）按 RFC3339 **只打印到整秒**，小数部分不显示，但小数秒在磁盘上和实际判断中都完整保留。所以不要用打印出来的整秒去手算间隔：两条打印时间一模一样的结果，可能一条恰好 60 秒（在线）、另一条已超过 60 秒（离线）。

下面的示例全部使用固定的接收/查询时间和同一个数据目录，结果不随执行时刻变化，可直接照做。较大序号记录先提交且采集时间更早，较小序号记录恢复后补传、采集时间更晚：

```bash
D=/tmp/edgefleet-readme-demo
rm -rf "$D"

# 断连期间，序号更大的 seq 10 先被收到；它采集于 11:58:00，11:58:30 被接收
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T11:58:30Z <<'EOF'
[{"node":"backfill","seq":10,"collected_at":"2026-10-01T11:58:00Z","version":"1.25.0","height":1000,"missed":5}]
EOF
# submitted: new=1 duplicate=0

# 节点恢复连接后补传较小的 seq 5；它采集时间更晚（11:59:50），12:00:00 才被接收
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T12:00:00Z <<'EOF'
[{"node":"backfill","seq":5,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":900,"missed":2}]
EOF
# submitted: new=1 duplicate=0   —— 补传成功只代表历史记录已接收

# 历史完整保留两条，按序号升序，互不覆盖
go run ./cmd/edgefleet heartbeat history --data-dir "$D" --node backfill
# node=backfill seq=5 collected_at=2026-10-01T11:59:50Z version=1.26.0 height=900 missed=2
# node=backfill seq=10 collected_at=2026-10-01T11:58:00Z version=1.25.0 height=1000 missed=5

# 健康查询仍取序号最大的 seq 10：采集于 11:58:00，到 12:00:00 已 120 秒，超过 60 秒
go run ./cmd/edgefleet heartbeat health --data-dir "$D" --node backfill \
  --expected-version 1.26.0 --tolerated-misses 4 --at 2026-10-01T12:00:00Z
# node=backfill status=offline seq=10 collected_at=2026-10-01T11:58:00Z version=1.25.0 height=1000 missed=5 findings=[offline version skew: 1.25.0 != 1.26.0 missed duties above tolerance]
# 退出码为 0：离线是一次成功的查询结果，不是命令出错
```

尽管 seq 5 采集更晚、接收也更晚，健康结果仍逐字段取自 seq 10——状态离线、版本 `1.25.0`、高度 `1000`、累计漏签 `5`（5 严格大于容差 4 才告警；seq 5 的 `missed=2` 本不会告警），seq 5 的任何字段都不会出现在结果里。

带小数秒的采集时间可以看清 60 秒边界两侧（仍用同一数据目录，另一个节点）：

```bash
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T12:02:00Z <<'EOF'
[{"node":"frac","seq":1,"collected_at":"2026-10-01T12:00:00.5Z","version":"1.0","height":100,"missed":0}]
EOF

# 间隔恰好 60 秒：在线
go run ./cmd/edgefleet heartbeat health --data-dir "$D" --node frac \
  --expected-version 1.0 --tolerated-misses 0 --at 2026-10-01T12:01:00.5Z
# node=frac status=online seq=1 collected_at=2026-10-01T12:00:00Z version=1.0 height=100 missed=0 findings=[]

# 只多 1 纳秒：离线（同样退出码 0）
go run ./cmd/edgefleet heartbeat health --data-dir "$D" --node frac \
  --expected-version 1.0 --tolerated-misses 0 --at 2026-10-01T12:01:00.500000001Z
# node=frac status=offline seq=1 collected_at=2026-10-01T12:00:00Z version=1.0 height=100 missed=0 findings=[offline]
```

两次打印的 `collected_at` 都是整秒 `2026-10-01T12:00:00Z`，区别只在输入里未被打印的 `.5` 与 `.500000001` 小数；判断用的是完整时刻。把同一时刻换成别的时区写法结论不变：`--at 2026-10-01T20:01:00.5+08:00` 仍为在线，`--at 2026-10-01T08:01:00.500000001-04:00` 仍为离线。小数秒完整保存在节点文件中，可用 `heartbeat history` 对照（其显示同样只到整秒）。

`--at` 还有一条硬约束：**查询时间早于最大序号记录的采集时间时，命令明确报错并以非零状态退出，不会回退改用任何较小序号的记录**，即使那条较小序号记录在查询时刻之前确已采集。下例中 seq 4 采集于 11:57:00、最大序号 seq 8 采集于 11:58:00，在 11:57:30 查询时 seq 4 已存在，但结果仍是报错：

```bash
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T12:00:00Z <<'EOF'
[
  {"node":"older","seq":4,"collected_at":"2026-10-01T11:57:00Z","version":"1.0","height":400,"missed":1},
  {"node":"older","seq":8,"collected_at":"2026-10-01T11:58:00Z","version":"1.0","height":404,"missed":3}
]
EOF

go run ./cmd/edgefleet heartbeat health --data-dir "$D" --node older \
  --expected-version 1.0 --tolerated-misses 0 --at 2026-10-01T11:57:30Z
# stderr: error: query time 2026-10-01T11:57:30Z is earlier than collection time 2026-10-01T11:58:00Z for node "older"
# 退出码 1，stdout 不打印任何健康结果，也不会改用 seq 4；加上 --missed-since-seq 4 同样报错。
# （通过 go run 运行时 stderr 还会附带一行 “exit status 1”，那是 go run 对非零退出的包装。）
# 查询时刻恰好等于最大序号采集时刻 11:58:00 则允许：间隔为 0，报在线，退出码 0。
```

这与上一节的漏签基准相互独立：`--missed-since-seq` 只改变漏签告警的统计口径（第二行的 `new_missed`），选中的记录始终是序号最大的那一条，在线状态始终只看它的采集时间到 `--at` 的间隔，首行的 `missed` 也始终是它的累计漏签数。基准既不会换记录，也不会把离线改成在线。

## 技术方向

validator, node-monitoring, device-fleet, p2p-network, telemetry, devnet

## 运行要求

Go 1.26，仅使用标准库，全部行为可在本机 CPU 上离线复现。
