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

节点标识长度不限：较短的标识保存为 `nodes/<标识的hex编码>.json`；标识过长、hex 名称会超过本机文件名上限时，保存为 `slots/<完整标识的sha256>.json`（定长内容名，与标识长度无关），所以 126 个 ASCII 字符、42 个汉字或由中文、表情、空白组成的更长标识都能正常提交和查询。两种存放方式只是文件位置不同，节点身份始终是用户提供的完整原文：不会因为长而被拒绝，也不会被截断、去空格或替换字符；两个只在末尾不同、共用很长前缀的标识即使序号相同也分别保存、互不相混；原文与等价 JSON 转义写法仍是同一节点。文件内的节点归属校验对两种位置同样生效——把另一个节点的完整心跳文件放到某标识对应位置，即使格式与校验和都正确，查询和继续提交也会明确拒绝，不会读成该节点的遥测，也不会覆盖已有记录。

节点标识和版本可以是任意合法 Unicode 文本（中文、表情、空格、换行均可提交、保存和查询）。健康查询输出时，不含空白、双引号、反斜杠或控制字符的文本按原样显示；其余文本显示为带双引号的 JSON 字符串（换行、回车、制表符显示为 `\n`、`\r`、`\t`，其他控制字符显示为 `\uXXXX`），因此每条健康结果始终只占一行，带引号的显示值按 JSON 字符串解读可还原原文。这只是展示规则：节点身份和版本比较仍使用原始文本，存储内容不会被改写。

### 补传一批记录：new、duplicate、记录冲突与写入失败

补传时一批里往往既有新心跳、也有此前已经提交过的心跳。`submit` 成功时打印一行计数：

```text
submitted: new=<新增数> duplicate=<重复数>
```

读懂这两个数，先要分清三件彼此独立的事：**记录身份**、**重复还是冲突**、以及**保存失败（写入失败）**。

**记录身份由“节点标识 + 序号”共同决定，缺一不可。** 序号不是全局编号：两个不同节点使用同一个序号是两条互不相干的记录，互不冲突、各自保存。只有节点标识和序号都相同，才指向同一条记录的身份。下面的 `peer-a` 与 `peer-b` 都用 `seq=1`，结果是两条新增：

```bash
D=/tmp/edgefleet-submit-demo
rm -rf "$D"
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T12:06:00Z <<'EOF'
[
  {"node":"peer-a","seq":1,"collected_at":"2026-10-01T12:05:00Z","version":"1.0","height":10,"missed":0},
  {"node":"peer-b","seq":1,"collected_at":"2026-10-01T12:05:30Z","version":"1.0","height":20,"missed":0}
]
EOF
# submitted: new=2 duplicate=0
```

**身份相同，还要逐字段比较内容，才能区分“重复”和“冲突”。**

- **重复（duplicate）**：节点、序号相同，且版本、区块高度、累计漏签数、真实采集时刻**全部一致**。采集时刻比较的是带纳秒精度的真实时刻，所以同一时刻用不同时区写法（如 `12:05:00Z` 与 `20:05:00+08:00`）仍是同一条；节点标识和版本按 JSON 解码后的原始文本比较，原文与等价的 `\uXXXX` 转义写法（例如 `"peer-a"`、`"1.0"` 分别写成 `"\u0070eer-\u0061"`、`"\u0031.\u0030"`，解码后仍是同一文本）也是同一条。重复记录不会再写一遍，只计数。
- **冲突（conflict）**：身份相同，但版本、高度、累计漏签数、采集时刻中**任意一项不同**。平台**不会**把后提交的内容当作“更新”覆盖旧记录——遥测以节点自己按序号报告的内容为准，同序号出现两种内容是必须暴露的矛盾，而不是静默替换。

**计数规则（重要：计的是输入里的“出现次数”，不是去重后的条数）：**

- 一条**此前已保存**的记录，在本批输入中出现几次，就计几次 `duplicate`；
- 一条**尚未保存**的相同记录在本批中出现多次：第一次计一次 `new`，其余每次都计 `duplicate`。即同一新记录在一批里出现三份，只保存一条，计数是 `new=1 duplicate=2`。

下面这个完整示例把两种重复放在同一批里。先保存一条 `seq=7`，再提交三份记录：一份是已有的 `seq=7`，另两份是内容完全相同的新心跳 `seq=8`：

```bash
D=/tmp/edgefleet-submit-demo
rm -rf "$D"

# 先保存一条心跳
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T12:00:00Z <<'EOF'
[{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":0}]
EOF
# submitted: new=1 duplicate=0

# 本批共提交 3 条：1 条与历史重复（seq 7），2 条彼此相同的新心跳（seq 8）
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T12:05:00Z <<'EOF'
[
  {"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":0},
  {"node":"val-eu-1","seq":8,"collected_at":"2026-10-01T12:04:00Z","version":"1.26.0","height":12350,"missed":0},
  {"node":"val-eu-1","seq":8,"collected_at":"2026-10-01T12:04:00Z","version":"1.26.0","height":12350,"missed":0}
]
EOF
# submitted: new=1 duplicate=2
```

对照“提交了多少条”和“实际保存了多少条”：这批输入里有 **3** 条记录，但历史只比提交前**多了 1 条**（`seq=8`）；`seq=7` 那份是历史重复，第二份 `seq=8` 是批内重复，二者合计 2 个 `duplicate`。历史始终按序号升序、每条序号只出现一次：

```bash
go run ./cmd/edgefleet heartbeat history --data-dir "$D" --node val-eu-1
# node=val-eu-1 seq=7 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=12345 missed=0
# node=val-eu-1 seq=8 collected_at=2026-10-01T12:04:00Z version=1.26.0 height=12350 missed=0
```

同理，把一条**已有**记录在一批里原样提交两次，会得到 `new=0 duplicate=2`——它出现了两次，就计两次。

**冲突会拒绝整批，而且冲突既可能发生在本批内部，也可能发生在输入与已有历史之间。** 任一位置出现同序号、不同内容的记录，命令都以非零状态退出，错误写到标准错误，标准输出不打印成功提示也不打印任何计数；**整批一条都不保存**：旧历史原样保留，本批里那些本来合法的新记录同样不会落盘。

先是“合法新心跳 + 与历史冲突的旧序号”：

```bash
# seq=9 是合法新记录，seq=7 与已保存内容不同（版本 1.27.0 != 1.26.0）
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T12:10:00Z <<'EOF'
[
  {"node":"val-eu-1","seq":9,"collected_at":"2026-10-01T12:09:00Z","version":"1.26.0","height":12360,"missed":0},
  {"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"1.27.0","height":12345,"missed":0}
]
EOF
# 退出码 1；通过 go run 运行时 stderr 还会附带一行 “exit status 1”
# stderr：
# error: conflicting record for node "val-eu-1" seq 7: content differs
# stdout：没有任何输出（没有 submitted 行，也没有 new/duplicate 计数）

# 历史不变：seq 7 仍是旧内容，本批的新记录 seq 9 也没有保存
go run ./cmd/edgefleet heartbeat history --data-dir "$D" --node val-eu-1
# node=val-eu-1 seq=7 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=12345 missed=0
# node=val-eu-1 seq=8 collected_at=2026-10-01T12:04:00Z version=1.26.0 height=12350 missed=0
```

再是“冲突只发生在本批内部”（一个此前没有任何遥测的新节点，两条同序号、不同高度）：

```bash
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" --receive-time 2026-10-01T12:10:00Z <<'EOF'
[
  {"node":"fresh","seq":1,"collected_at":"2026-10-01T12:08:00Z","version":"1.0","height":1,"missed":0},
  {"node":"fresh","seq":1,"collected_at":"2026-10-01T12:08:00Z","version":"1.0","height":2,"missed":0}
]
EOF
# 退出码 1，stdout 无输出，stderr：
# error: conflicting record for node "fresh" seq 1: content differs

go run ./cmd/edgefleet heartbeat history --data-dir "$D" --node fresh
# node=fresh no heartbeats
```

这与字段非法、采集时间越界等情况的处理一致：凡是在写入前就能判定的问题，都是“整批拒绝、零改动”。

**写入失败与冲突不是一回事。** 冲突（以及非法输入）在任何数据落盘之前就能判定，所以能干净地整批拒绝；而写入失败发生在真正持久化阶段——数据按节点分文件逐个写入，磁盘写满、I/O 错误等可能在**中途**出现。此时命令同样报告失败并以非零状态退出，但**排在失败之前的某些节点可能已经把新记录保存了**。因此遇到写入失败时：

- **不能**假设所有新增都已撤销，也**不能**假设全都没保存——哪些节点已落盘并不由输入顺序承诺；
- **此前**已经保存的记录一律保留，单个节点文件始终原子替换，不会出现写坏一半的文件；
- 正确做法是：先用 `heartbeat history` 逐个节点核对现状，再把**同一批 JSON 原样重新提交**。重提是幂等的——已经保存的相同记录会计为 `duplicate`，只有尚未保存的记录才计为 `new`，不会产生重复行。

下面在本机离线制造一次“写到一半磁盘写满”（仅演示用：借助 Linux user namespace 挂一个只放得下一个节点文件的极小 tmpfs，无需 root；在不支持 user namespace 的环境可跳过注入，结论不变）：

```bash
D=/tmp/edgefleet-write-fail
cat > /tmp/edgefleet-batch.json <<'EOF'
[
  {"node":"alpha","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0},
  {"node":"beta","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":200,"missed":0}
]
EOF

unshare -rm bash -s /tmp/edgefleet-batch.json <<'NS'
set +e
D=/tmp/edgefleet-write-fail
BATCH="$1"
mkdir -p "$D"
mount -t tmpfs -o size=4096 none "$D"          # 4KiB，只够放下一个节点文件

# 提交两个节点：写到第二个时空间耗尽
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" \
  --receive-time 2026-10-01T12:00:00Z < "$BATCH"
# 退出码 1，stdout 无计数，stderr 形如：
# error: failed to persist heartbeat batch: write .../nodes/.tmp-xxxxxxxxxx: no space left on device

# 第一步：逐个节点查历史，确认到底谁已经保存（具体是 alpha 还是 beta 取决于写入顺序）
for n in alpha beta; do
  printf '%s -> ' "$n"
  go run ./cmd/edgefleet heartbeat history --data-dir "$D" --node "$n"
done
# 可能的输出之一（另一个节点显示 no heartbeats；你机器上先落盘的可能是 beta）：
# alpha -> node=alpha seq=1 collected_at=2026-10-01T11:59:00Z version=1.0 height=100 missed=0
# beta -> node=beta no heartbeats

# 第二步：排除故障（这里把空间放大），把同一批 JSON 原样重新提交
mount -o remount,size=1048576 none "$D"
go run ./cmd/edgefleet heartbeat submit --data-dir "$D" \
  --receive-time 2026-10-01T12:00:00Z < "$BATCH"
# submitted: new=1 duplicate=1     —— 已落盘的计重复，未落盘的计新增，与谁先落盘无关

# 两个节点最终都恰好各有一条，没有重复行
for n in alpha beta; do
  go run ./cmd/edgefleet heartbeat history --data-dir "$D" --node "$n"
done
# node=alpha seq=1 collected_at=2026-10-01T11:59:00Z version=1.0 height=100 missed=0
# node=beta seq=1 collected_at=2026-10-01T11:59:00Z version=1.0 height=200 missed=0
NS
```

**保存的最后一步还要“确认目录”。** 每个节点文件都是先写临时文件、fsync、再原子替换到正式位置；替换之后，平台会打开该文件所在目录并对目录做一次 fsync，确认这次替换真正落盘（短标识在 `nodes/`、长标识在 `slots/`）。这一步不是可有可无的附加动作：**文件已经替换之后，如果打不开所在目录，或目录同步到磁盘时返回错误，本次提交同样按保存失败处理**——命令以非零状态退出，标准输出不打印成功提示也不打印任何计数，标准错误明确说明失败发生在“保存确认”阶段，并指出涉及的目录与具体原因，例如：

```text
error: failed to persist heartbeat batch: save confirmation failed in directory /data/nodes: cannot open directory to confirm node-file replacement: open /data/nodes: permission denied
error: failed to persist heartbeat batch: save confirmation failed in directory /data/slots: cannot sync directory to disk after node-file replacement: input/output error
```

这种失败有三点必须辨清：

- 它发生在**新节点文件已经替换之后**，所以不会把文件删掉或恢复成旧内容来伪造“全部未保存”：已经替换的节点，其**完整记录**照常可被 `heartbeat history` 读到（文件本身始终完整，不存在只能读一半的心跳文件）；尚未轮到的节点则保持实际进度。上例两个节点各加一条 `seq=2` 时，可能一个节点的两条记录都已落盘、另一个节点仍只有 `seq=1`，具体哪个先保存不作承诺。
- 它属于**保存阶段的错误**，不是心跳内容冲突、字段非法，也不是存储数据损坏——文件内容完整且校验正常，只是持久性没能确认；因此不要按冲突或损坏的方式处理（不要清数据、不要改内容），排查的是该目录的权限/挂载/磁盘。
- 处理方式与其他写入失败相同：故障排除后把**同一批 JSON 原样重新提交**，已保存的记录计为 `duplicate`、未保存的计为 `new`，结果幂等收敛。

通过 Go 的 `Store.Submit` 接口调用时行为一致：返回保存确认错误（`IsSaveConfirm(err)` 为真），且 `newCount`、`dupCount` 均为 0——不会把已经落盘的那部分记录当作本批成功数。**只由已保存记录的原样重复组成的提交不重写节点文件，因此也不会执行这一步目录确认**：全部重复的批次照常只计 `duplicate`，混合批次里的纯重复节点同样保持原文件，都不会因为目录暂时无法确认而失败；非法输入、同节点同序号内容冲突仍在写入前整批拒绝，与本阶段无关。

一句话区分：**冲突是“内容矛盾”，写入前就能判定，整批零改动；写入失败是“持久化中途出错”（含替换后目录打不开或目录 fsync 失败），可能已有部分节点保存，需查历史后原样重提。** 两种情况下命令都以非零状态退出、错误进 stderr，且都不会用后提交的内容覆盖旧记录。

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
- **查询时间**（健康查询的 `--at`）：本次判断所站的时刻；省略 `--at` 时取当前时间。显式给出的 `--at` 必须是带时区的 RFC3339 时间，并采用与 `collected_at` 相同的限制：数字时区偏移小时 `00` 至 `23`、分钟 `00` 至 `59`，越界分钟不折算（`+00:60` 拒绝），`+24:00`、`-24:00` 拒绝，`Z`、`-00:00`、`±23:59` 合法；小数秒（`.` 或 `,` 分隔）最多九位完整保留，超过九位仅在第十位起全部为零时接受，之后任意一位非零即拒绝，绝不截断或四舍五入后再判断。不符合时命令以非零状态退出，stdout 不打印健康结果或漏签基准说明，stderr 指明 `--at` 及有问题的偏移或小数秒；即使节点尚无遥测或本次带有漏签基准也同样先检查查询时间，已保存心跳不受影响。

因此，较小序号心跳在节点恢复连接后补传成功（`new=1`），只表示那条历史记录已被接收并保存，**不表示节点重新在线**。在线状态看的是“选中记录的采集时间 → 查询时间”的间隔，与接收时间无关：间隔不超过 60 秒为在线，**恰好 60 秒仍为在线，超过 60 秒即为离线**。节点要重新显示在线，需要再收到一条序号更大、且采集时间距查询时刻不超过 60 秒的新心跳。

采集时间必须能用保存格式**原样**保留，否则整批拒绝、一条都不保存：年份只能是四位的 `0000` 至 `9999`；数字时区偏移的小时位只能是 `00` 至 `23`、分钟位只能是 `00` 至 `59`，且偏移只能到整分钟、不带秒。因此 `+24:00`、`-24:00` 会明确报错，`+00:60`、`+23:60` 这类越界分钟**不会被折算**成 `+01:00`、`+24:00` 后接收；通过 Go 接口直接构造心跳时，年份越界（小于 0000 或大于 9999）、偏移超过 ±23:59、或带秒数而非整分钟的偏移同样拒绝。只要批中有一条这样的记录，其余合法记录和重复记录都不会产生任何保存变化：已有节点的历史保持原样，新节点仍无遥测，命令以非零状态退出，stdout 不打印成功或新增/重复数量，stderr 指出记录位置和 `collected_at` 的具体问题；直接调用 `Submit` 的调用方收到能确定节点和序号的错误，新增与重复数量均为零。`Z`、`-00:00`、`±23:59`、`+08:00`、以及四位年份边界 `0000`、`9999` 都是合法值；小数秒（纳秒精度）继续支持，同一真实时刻的不同时区写法仍按同一条心跳判断重复。

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
