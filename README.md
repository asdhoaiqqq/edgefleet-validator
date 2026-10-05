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

## 技术方向

validator, node-monitoring, device-fleet, p2p-network, telemetry, devnet

## 运行要求

Go 1.26，仅使用标准库，全部行为可在本机 CPU 上离线复现。
