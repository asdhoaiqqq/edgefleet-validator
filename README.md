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
- 第 2 行：事件时间 1000、值 3。区间左闭右开，时间恰好等于前一个窗口的右端点，所以 1000 **不属于 `[0,1000)`**；按起点 0、600、1200……排列，包含 1000 的窗口只有 `[600,1600)`（600 ≤ 1000 < 1600）——并不存在起点为 1000 的窗口，而下一个窗口 `[1200,2200)` 要到时间 1200 才开始，也不包含它。此时仍没有水位线，没有输出。
- 第 3 行：水位线推进到 1000。所有 end ≤ 1000 的窗口关闭，符合条件的只有 `[0,1000)`（end 恰好等于 1000，取等关闭）；其中只有第 1 行的事件，因此输出 count=1、sum=2。`[600,1600)` 的 end 为 1600，大于水位线，仍然继续等待，这就是标准输出的第一行。
- 第 4 行：事件时间 999、值 9。999 虽然仍落在尚未关闭的重叠窗口 `[600,1600)` 之内，但迟到判断先于入窗：999 严格低于当前水位线 1000，事件被**整体跳过**，不进入任何窗口，标准错误按物理行号报告迟到原因（见上面的标准错误行）。它的值 9 不会贡献给任何计数或求和。
- 第 5 行：事件时间 1000、值 4。时间**恰好等于当前水位线**，规则只拒绝严格低于水位线的事件，所以这条仍被正常接收；与第 2 行同理，它只进入 `[600,1600)` 一个窗口，没有输出。
- 第 6 行：水位线推进到 1600，关闭 `[600,1600)`（已关闭的 `[0,1000)` 不会重复输出，下一个窗口 `[1200,2200)` 的 end 为 2200，仍要等待）。这个窗口里是第 1、2、5 行的三条事件（时间 700/值 2、1000/值 3、1000/值 4），所以 count=3、sum=2+3+4=9；第 4 行被跳过的事件不在其中。这就是标准输出的第二行。
- 第 7 行：事件时间 1700、值 5。1700 不低于当前水位线 1600，是合法事件；按窗口排列，包含 1700 的窗口只有 `[1200,2200)`（1200 ≤ 1700 < 2200）——`[600,1600)` 的右端点 1600 已在它之前，而下一个窗口 `[1800,2800)` 的起点 1800 还未到，所以它只进入这一个窗口。随后输入正常结束，而 `[1200,2200)` 的 end 为 2200，大于当前水位线 1600；**结束输入不会补发任何仍未关闭的窗口**，所以这个窗口不产生任何输出，第 7 行的事件也不会出现在任何结果行中。第 4 行的迟到提示只是标准错误上的一条说明，并不是运行失败：整个进程退出码仍为 0。

### 延续：在输入结束前再追加一条水位线 2200

若在上面第 7 行事件之后、输入结束之前再追加一条水位线 2200，`[1200,2200)` 就会关闭。下面是包含全部 8 行的完整输入，可直接运行，无需另起命令只补发一条水位线：

```bash
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":700,"value":2}' \
  '{"type":"event","key":"sensor-a","time":1000,"value":3}' \
  '{"type":"watermark","time":1000}' \
  '{"type":"event","key":"sensor-a","time":999,"value":9}' \
  '{"type":"event","key":"sensor-a","time":1000,"value":4}' \
  '{"type":"watermark","time":1600}' \
  '{"type":"event","key":"sensor-a","time":1700,"value":5}' \
  '{"type":"watermark","time":2200}' \
  | go run ./cmd/edgefleet aggregate --window-ms 1000 --slide-ms 600
```

标准输出在原有两行之后只**新增一行**（水位线再次推进不会让已关闭的两个窗口重发）：

```json
{"key":"sensor-a","start":1200,"end":2200,"count":1,"sum":5}
```

这一行只来自第 7 行时间 1700、值 5 的事件——它是唯一进入 `[1200,2200)` 的事件，所以 count=1、sum=5；第 1、2、5 行的事件时间都不在该区间内，第 4 行此前已被整体跳过，都不会贡献进来。标准错误仍是原来那一行迟到提示，退出码仍为 0。

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

## 示例：事件 key 的转义写法——什么情况下合并计数，什么情况下分成两个窗口

前面的命令行示例都用 `sensor-a` 这类不含特殊字符的普通 key，读者很难判断两条字符串写法不同的记录究竟是合并计数，还是成为两个窗口结果。事件的 `key` 是一个 JSON 字符串，程序按 **JSON 解码之后的字符串** 分组，而不是按输入行里那串字面文本分组。读这类示例时要分清三层，三层出现的反斜杠数量不同：

1. **你在命令行里写下、由 shell 单引号原样交出的文本**。本节命令一律用单引号 `'...'` 包住每条记录；单引号内不做任何转义处理，所以你看到几个反斜杠，程序就收到几个。
2. **程序实际读到的输入字节**。例如 key 位置写下 `a\nb` 时，这一行的字节就是 `a`、`\`、`n`、`b`，反斜杠是真实字节 0x5C，记录本身并没有真的换行，仍占一个物理输入行。
3. **JSON 解码之后参与分组的字符**。JSON 把 `\n` 解码成一个换行字符（0x0A），把 `\\` 解码成一个反斜杠字符（0x5C）。**窗口按这一层的字符串合并与排序**：两条记录是否同一个 key，只取决于这一层是否逐字符相同。

### 成功示例：同一换行字符的两种写法合并，真正的"反斜杠＋n"单独汇总

1000 毫秒固定窗口（省略 `--slide-ms`）、单水位线（不带分区）。三条事件的时间 100/200/300 都落在同一个窗口 `[0,1000)` 内，最后一条水位线 1000 关闭它。前两条记录用两种合法写法表示**同一个含换行字符的 key**（一种用 `\n`，另一种用 `\u000a`），第三条在同一位置写的是**反斜杠加字母 n 这两个普通字符**；三条事件的 value 分别取 2、3、7，便于直接复核。命令可直接离线运行，并用构建出的二进制观察程序自身退出码：

```bash
go build -o /tmp/edgefleet ./cmd/edgefleet
printf '%s\n' \
  '{"type":"event","key":"a\nb","time":100,"value":2}' \
  '{"type":"event","key":"a\u000ab","time":200,"value":3}' \
  '{"type":"event","key":"a\\nb","time":300,"value":7}' \
  '{"type":"watermark","time":1000}' \
  | /tmp/edgefleet aggregate --window-ms 1000
echo "exit=$?"
```

完整标准输出恰好为两行：

```json
{"key":"a\nb","start":0,"end":1000,"count":2,"sum":5}
{"key":"a\\nb","start":0,"end":1000,"count":1,"sum":7}
```

完整标准错误为空（零字节：没有迟到事件，也没有损坏记录）。程序自身退出码为 0：

```
exit=0
```

三条记录逐层对照（"输入字节"指 key 引号内程序实际读到的字节）：

| 输入行 | 单引号内的写法 | key 引号内的输入字节 | JSON 解码后的 key | 归属 |
| --- | --- | --- | --- | --- |
| 第 1 行 | `"a\nb"` | `a` `\` `n` `b` | `a`＋换行(0x0A)＋`b` | 换行 key |
| 第 2 行 | `"a\u000ab"` | `a` `\` `u` `0` `0` `0` `a` `b` | `a`＋换行(0x0A)＋`b` | 同一个换行 key |
| 第 3 行 | `"a\\nb"` | `a` `\` `\` `n` `b` | `a`＋反斜杠(0x5C)＋字母`n`＋`b` | 另一个 key |

- 第 1 行的 `\n` 是 JSON 的换行短转义，解码成一个换行字符；第 2 行的 `\u000a` 是同一个换行字符的 Unicode 转义写法（码位 U+000A 即换行）。两者**解码结果逐字符相同**，所以是同一个 key，在 `[0,1000)` 内合并计数：`count=2`、`sum=2+3=5`。输入文本长短不同（`\n` 占两个字节、`\u000a` 占六个字节）不影响分组。
- 第 3 行的 `\\` 是 JSON 的反斜杠转义，只解码成**一个**反斜杠字符，其后的 `n` 是普通字母、不再参与任何转义，所以这个 key 在该位置是"反斜杠＋字母 n"两个字符。它**不会被再多解码一次**而变成换行 key，因此单独汇总：`count=1`、`sum=7`。

### 结果为什么按这个次序输出

同一条水位线一次关闭多个 key 的窗口时，先按窗口 end 升序、end 相同时再按**解码后的 key 的 UTF-8 字节顺序**排列。这个次序与输入记录的先后、与转义文本的长短都无关。本例两个窗口的 end 都是 1000，两个 key 的首字节同为 `a`，下一字节一个是换行 0x0A、一个是反斜杠 0x5C，而 `0x0A < 0x5C`，所以含换行的 key 排在前面。把三条事件任意调换先后顺序（例如先喂第 3 行、再喂第 1、2 行），输出的两行内容与次序完全不变。

### 输出里换行仍以 JSON 转义呈现，每个窗口结果只占一个物理行

结果 JSON 与输入遵循同一套转义：第一行结果 key 里的换行字符在输出上仍是两字符序列 `\n`（字节 0x5C 紧接 0x6E），不是原始字节 0x0A；第二行结果里那个真正的反斜杠转义成 `\\`，所以两行在纸面上绝不会看起来一样。整份标准输出只有两个换行字节，分别位于两行结果的末尾，**每个窗口结果恰好占一个物理输出行**，key 内部不含任何原始控制字节。

再次读取该行 JSON 时，key 应还原出原来的字符，而真正的反斜杠不会被多解码一次。本机有 python3 时可用下面的命令复核（`repr` 会把真正的换行显示成 `\n`，把"反斜杠＋字母 n"显示成 `\\n`）：

```bash
printf '%s\n' \
  '{"type":"event","key":"a\nb","time":100,"value":2}' \
  '{"type":"event","key":"a\u000ab","time":200,"value":3}' \
  '{"type":"event","key":"a\\nb","time":300,"value":7}' \
  '{"type":"watermark","time":1000}' \
  | /tmp/edgefleet aggregate --window-ms 1000 \
  | python3 -c 'import sys,json
for line in sys.stdin:
    o=json.loads(line); print(repr(o["key"]), o["count"], o["sum"])'
```

```text
'a\nb' 2 5
'a\\nb' 1 7
```

即第一行的 key 里确实是一个换行字符，第二行确实是一个反斜杠加字母 n；后者没有被二次解码成换行，两行也没有合并。

**复制命令时请保持单引号和反斜杠与上面逐字对应。** 本节用单引号包住每条记录，shell 不会改动其中的反斜杠。若把某条记录挪进双引号并按双引号习惯转义内部引号，shell 会先吃掉一层反斜杠：第三条若写成 `"{\"type\":\"event\",\"key\":\"a\\nb\",\"time\":300,\"value\":7}"`，程序收到的就变成 `a\nb`（真正的换行 key），三条事件会**静默合并成一行 `count=3、sum=12`**——这是另一份数据，而且不会报错。所以不要把记录挪进双引号，也不要自行增减反斜杠，以免复制后得到与文档不同的输入。

### 失败示例：未配对的代理项是输入错误，不是普通迟到跳过

下面是一个独立示例，共 4 行：先用一条事件和一条水位线正常关闭一个合法窗口；随后输入一条 key 含**未配对 Unicode 代理项**、且事件时间低于当前水位线的记录；最后再给一条水位线。

```bash
go build -o /tmp/edgefleet ./cmd/edgefleet
printf '%s\n' \
  '{"type":"event","key":"ok","time":100,"value":5}' \
  '{"type":"watermark","time":1000}' \
  '{"type":"event","key":"bad\ud800key","time":999,"value":1}' \
  '{"type":"watermark","time":2000}' \
  | /tmp/edgefleet aggregate --window-ms 1000
echo "exit=$?"
```

终端里标准输出（第 2 行水位线触发）先于标准错误（第 3 行记录触发）。分开列出：完整标准输出只有此前已关闭的那一个窗口，并且被原样保留：

```json
{"key":"ok","start":0,"end":1000,"count":1,"sum":5}
```

完整标准错误恰好一行，指明出错的**物理行号**与字符损坏原因：

```
aggregate: line 3: field "key" has an unpaired high surrogate in a Unicode escape
```

程序自身退出码为 1：

```
exit=1
```

逐行说明（行号即物理输入行号）：

- 第 1 行：合法事件，进入窗口 `[0,1000)`。
- 第 2 行：水位线 1000 关闭 `[0,1000)`，输出 `count=1、sum=5`，当前水位线变为 1000。
- 第 3 行：**物理第 3 行**即出错行。`\ud800` 落在高代理区间 U+D800–U+DBFF；按 JSON 规则，它后面必须紧跟另一个 `\uXXXX` 转义给出低代理项（U+DC00–U+DFFF），两个半区合起来才表示一个辅助平面字符。单独一个高代理项不对应任何 Unicode 字符，属于**字符编码损坏**。程序对 key 的解码是严格的：宁可失败也不把它替换成 U+FFFD 之类的替代字符——否则不同的损坏 key 可能彼此合并，还可能与真正是 U+FFFD 的 key 相撞。尽管这行的事件时间 999 也严格低于当前水位线 1000，**但 key 损坏检查先于迟到判断**，所以它是致命的输入错误（退出码 1），**而不是**普通迟到事件那种"在标准错误写一行迟到提示、跳过该事件、继续运行（退出码 0）"。对照可见：若只把这行的 key 换成合法的 `"ok"`、时间仍是 999，标准错误只会得到 `line 3: late event time=999 below current watermark 1000, skipped`，进程以 0 退出——两者是不同性质的处理。
- 第 4 行：致命错误之后停止读取后续记录，这条水位线 2000 不会再被处理，也**不会再产生任何输出**。标准输出停在第 2 行已写出的那一行，进程以退出码 1 失败退出。

若改用 `go run ./cmd/edgefleet ...` 运行，`go run` 对非零退出统一报成退出码 1 并多打印一行 `exit status 1`；这里程序自身退出码本就是 1，构建二进制后即可直接观察到上面的结果。

## 示例：累计求和的取值范围与即时溢出

这一节专门说明同一个 key 在重叠窗口中的 `sum`：每条事件的值合法，并不等于把它加进每个窗口后的累计也合法。命令参数、逐行 JSON 输入格式、左闭右开的窗口划分和"只由水位线关闭、结束输入不补发"的规则都与前几节相同，这里只补充数值范围与失败时机。

先分清两层合法性，它们在不同时机检查：

- **每条 `value` 都合法**：事件的 `value` 字段本身必须能解析成一个有符号六十四位整数（`int64`），范围是 `[-9223372036854775808, 9223372036854775807]`。字段超出这个范围（例如 `9223372036854775808`）在解析这一物理行时就是记录错误，事件不会进入任何窗口，与下面的累计溢出是两类不同的失败。
- **每次累计也合法**：事件通过字段检查后，要把**完整的 value** 分别加进**每一个**包含其事件时间的窗口。各窗口的 `count`、`sum` 彼此独立记账：一条事件属于几个窗口，就在几个窗口里各计一次数、各加一次完整的 value，**不会**因为窗口重叠而把 value 按重叠数量分摊。某窗口本次加法的结果仍落在 `int64` 范围内才算合法。

关于累计的判定时机与边界：

- `value` 和窗口的 `sum` 都是有符号六十四位整数。检查发生在**每一次加法之前**，而不是等窗口关闭后才回头检查最终总和：某次加法恰好把累计带到上限 `9223372036854775807` 或下限 `-9223372036854775808` 仍然合法；一旦某次加法的结果越过任一端点，本次运行**立即**在这条事件上失败并停止读取后续记录，不会等到关闭窗口。
- 因为各窗口独立累计，**同一条自身合法的事件，可能在一个包含它的窗口里安全加入、在另一个包含它的窗口里造成溢出**；错误信息会点名实际越界的那一个窗口，而不是笼统归咎于这条事件碰到的所有重叠窗口。
- 负值可以抵消同一窗口里此前已接收的正值（总和随之回落）。但抵消只在事件被实际处理时生效：**致命错误之后才出现的抵消事件不会被读取，无法挽回这次运行**。
- 合法的大整数在结果 JSON 中始终是**精确的整数值**：直接写成十进制数字，不加引号变成字符串，也不经过浮点表示，因此不存在浮点舍入（大于 2 的 53 次方 `9007199254740992` 的整数照样原样输出）。

### 成功示例：重叠窗口中的大整数与正负抵消

窗口长度 1000、滑动间隔 600，窗口依次为 `[0,1000)`、`[600,1600)`、`[1200,2200)`、`[1800,2800)`……同一个 key `sensor-a`，共 6 行输入。第 1 行的值 `9007199254740993` 等于 2 的 53 次方加 1，是合法的 `int64`；第 4 行用负值 `-97` 抵消此前的正值。下面的命令可独立离线运行，并用构建出的二进制直接观察程序自身退出码：

```bash
go build -o /tmp/edgefleet ./cmd/edgefleet
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":700,"value":9007199254740993}' \
  '{"type":"event","key":"sensor-a","time":1000,"value":100}' \
  '{"type":"watermark","time":1000}' \
  '{"type":"event","key":"sensor-a","time":1300,"value":-97}' \
  '{"type":"watermark","time":1600}' \
  '{"type":"watermark","time":2200}' \
  | /tmp/edgefleet aggregate --window-ms 1000 --slide-ms 600
echo "exit=$?"
```

标准输出恰好为三行（标准错误为空，退出码为 0）：

```json
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":9007199254740993}
{"key":"sensor-a","start":600,"end":1600,"count":3,"sum":9007199254740996}
{"key":"sensor-a","start":1200,"end":2200,"count":1,"sum":-97}
```

```
exit=0
```

可按事件所属区间逐窗复核（每条事件把完整 value 加入它所属的每个窗口）：

- 第 1 行：时间 700 同时属于 `[0,1000)` 和 `[600,1600)`，两个窗口各加一次完整的 `9007199254740993`，不分摊。
- 第 2 行：时间 1000 是 `[0,1000)` 的右端点（左闭右开，不含），只属于 `[600,1600)`，在该窗加 `100`。
- 第 3 行：水位线 1000 关闭 `[0,1000)`，其中只有第 1 行，故 `count=1`、`sum=9007199254740993`——这个大于 2 的 53 次方的整数原样输出为精确数值。
- 第 4 行：时间 1300 同时属于 `[600,1600)` 和 `[1200,2200)`，在两个窗口各加 `-97`；它在 `[600,1600)` 中把累计从 `9007199254741093` 抵消回 `9007199254740996`。
- 第 5 行：水位线 1600 关闭 `[600,1600)`，其中是第 1、2、4 行三条事件，`count=3`、`sum=9007199254740993+100-97=9007199254740996`。
- 第 6 行：水位线 2200 关闭 `[1200,2200)`，其中只有第 4 行，`count=1`、`sum=-97`。

### 失败示例：一次安全累加、一次累计溢出，以及迟到的大值

下面 8 行输入（**第 2 行是空行，空行也计入物理行号**）先用水位线输出一个窗口，随后用一条迟到的大值事件说明迟到与溢出的区别，再用一条非迟到事件在较早的窗口安全累加、在较晚的窗口触发累计溢出；出错记录之后还安排了抵消事件和水位线，用来说明它们都不再生效。记 `M = 9223372036854775807`（`int64` 上限）。

```bash
go build -o /tmp/edgefleet ./cmd/edgefleet
printf '%s\n' \
  '{"type":"event","key":"sensor-a","time":100,"value":10}' \
  '' \
  '{"type":"watermark","time":1000}' \
  '{"type":"event","key":"sensor-a","time":999,"value":999999999999999999}' \
  '{"type":"event","key":"sensor-a","time":1700,"value":9223372036854775802}' \
  '{"type":"event","key":"sensor-a","time":1300,"value":10}' \
  '{"type":"event","key":"sensor-a","time":1400,"value":-10}' \
  '{"type":"watermark","time":2200}' \
  | /tmp/edgefleet aggregate --window-ms 1000 --slide-ms 600
echo "exit=$?"
```

终端里两类输出按处理顺序交错（行尾注释标明流和触发它的输入行）：

```
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":10}                                              # 标准输出（第 3 行水位线触发）
line 4: late event time=999 below current watermark 1000, skipped                                       # 标准错误（第 4 行迟到事件）
aggregate: line 6: cumulative sum overflow for key "sensor-a" window [1200,2200): 9223372036854775802 + 10   # 标准错误（第 6 行累计溢出）
```

分开看，标准输出恰好一行——出错前已经写出的结果保留：

```json
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":10}
```

标准错误恰好两行：

```
line 4: late event time=999 below current watermark 1000, skipped
aggregate: line 6: cumulative sum overflow for key "sensor-a" window [1200,2200): 9223372036854775802 + 10
```

```
exit=1
```

逐行说明（行号即物理输入行号，第 2 行空行也占一个行号）：

- 第 1 行：时间 100 只属于 `[0,1000)`，加 `10`。
- 第 3 行：水位线 1000 关闭 `[0,1000)`，输出 `count=1`、`sum=10`，当前水位线变为 1000。
- 第 4 行：时间 999、值 `999999999999999999`。它的 `value` 字段本身完全合法（小于 `M`），但**迟到判断先于入窗**：999 严格低于当前水位线 1000，整条事件被跳过并只在标准错误报告迟到，不进入任何窗口、不参与求和。**不能因为它的值很大就把它解释成累计溢出**——它连加法都没有发生。
- 第 5 行：时间 1700、值 `M-5 = 9223372036854775802`，非迟到。包含 1700 的窗口只有 `[1200,2200)`（1200 ≤ 1700 < 2200；`[600,1600)` 的右端点 1600 已在它之前，`[1800,2800)` 的起点 1800 还没到）。该窗累计从 0 变为 `M-5`，仍在上限以内，合法。
- 第 6 行：时间 1300、值 `10`，非迟到。它同时属于 `[600,1600)` 和 `[1200,2200)`，程序按窗口起点升序逐个相加：在较早的 `[600,1600)` 中是 `0 + 10 = 10`，**安全累加**；在较晚的 `[1200,2200)` 中是 `(M-5) + 10 = M+5`，越过上限，**立即失败**。所以致命的是 `[1200,2200)` 这一个窗口——标准错误明确给出物理行号 `line 6`、key `sensor-a`、窗口区间 `[1200,2200)`，以及触发溢出的两个加数（该窗此前的累计 `9223372036854775802` 和本事件的值 `10`）；较早窗口 `[600,1600)` 的那次安全加法不受指责，错误也不会笼统归咎于两个重叠窗口。
- 第 7 行：时间 1400、值 `-10`，本可以进入 `[1200,2200)` 等窗口做抵消，但它出现在第 6 行致命错误**之后**，不会再被读取，因此**无法抵消、无法挽回这次运行**。
- 第 8 行：水位线 2200 同样在出错之后，不再生效。于是即使它本可关闭 `[600,1600)`（end 1600 ≤ 2200，第 6 行已在其中安全累加到 `sum=10`）和 `[1200,2200)`，这两个仍未关闭的窗口也**不会被补发**；标准输出停在第 3 行已写出的那一行，进程退出码为 1。

这里程序自身的退出码本就是 1；若改用 `go run` 运行，`go run` 对非零退出也报 1 并多打印一行 `exit status 1`，用上面的二进制即可直接看到程序自身的退出码。

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

## 示例：通过 Go 库接入输入流——正常结束与读取失败的区分

前面的示例都通过 `edgefleet aggregate` 命令从标准输入读数据。把聚合功能嵌进自己的 Go 程序时，入口是库函数 `edgefleet.RunAggregate`（固定窗口；滑动、分区变体分别是 `RunAggregateSliding`、`RunAggregatePartitioned`、`RunAggregatePartitionedSliding`，读取边界规则完全相同）：

```go
func RunAggregate(r io.Reader, windowMillis int64, out io.Writer, lateLog io.Writer) error
```

`r` 是任意 `io.Reader`（网络连接、管道、消息队列客户端都可以），`out` 接收每行一个的窗口结果 JSON，`lateLog` 接收迟到事件提示，二者都可以直接给内存里的 `bytes.Buffer`，不涉及文件、网络和当前时间。本节示例代码位于 [`examples/aggregate-read/main.go`](examples/aggregate-read/main.go)，可在本机离线直接运行：

```bash
go run ./examples/aggregate-read
```

### 读取器同时交出字节和错误时，哪些生效、哪些舍弃

记录边界是**换行符，不是 `Read` 调用的边界**：一次 `Read` 可以只来半条记录，也可以一次带回多条记录。因此当读取器在最后一批字节上同时返回 `n > 0` 和一个错误时，需要区分这批内容里的两部分：

- **已经收到换行符的完整记录**（包括与错误同一次 `Read` 带回的完整记录）按顺序逐条生效：事件入窗、水位线推进并关闭窗口，已关闭窗口的结果行已经写入 `out` 就保留。
- **最后一个换行符之后的未结束尾部必须舍弃**：它既不是事件也不是水位线，不会入窗、不会关闭窗口、不会产生迟到提示，也**不会**被当成"某一行 JSON 不合法"报成记录错误。
- 之后把读取器自己的错误**原样**作为 `RunAggregate` 的返回值交还调用方（不包装、不替换），所以调用方可以用 `errors.Is` 识别输入源报告的真实原因。

正常结束与读取故障的判定只有一条：

| 读取器最后的返回 | 含义 | 无换行的最后一条记录 | `RunAggregate` 返回 |
| --- | --- | --- | --- |
| 裸 `io.EOF`（`err == io.EOF`） | 正常结束 | 仍作为最后一条完整记录处理 | `nil` |
| 任何其他错误（含**包装过的** `io.EOF`、错误链里能 `errors.Is` 到 `io.EOF` 的组合错误） | 读取故障 | 舍弃，不生效 | 读取器的原始错误，原样返回 |

注意：判定正常结束只能用错误身份比较（在读取器实现里即 `return io.EOF`），**不能用 `errors.Is(err, io.EOF)`**。`fmt.Errorf("...: %w", io.EOF)` 包装出来的 EOF、或同时挂着 `io.EOF` 和真实故障原因的组合错误，即使 `errors.Is` 能找到 `io.EOF`，也一律按读取故障处理。

### 同一份输入的两种结束方式

示例围绕同一份字节流对比（1000 毫秒固定窗口，同一个 key `sensor-a`，共 4 个物理行，**最后一行故意不带换行符**）：

```text
line 1: {"type":"event","key":"sensor-a","time":100,"value":5}      + "\n"
line 2: {"type":"watermark","time":1000}                            + "\n"
line 3: {"type":"event","key":"sensor-a","time":1100,"value":7}    + "\n"
line 4: {"type":"watermark","time":2000}                            (无换行)
```

- 第 1 行事件时间 100，落在窗口 **`[0,1000)`**（左闭右开）内。
- 第 2 行水位线 1000（有换行），关闭 end ≤ 1000 的窗口，即 `[0,1000)`。
- 第 3 行事件时间 1100，落在窗口 **`[1000,2000)`** 内。
- 第 4 行水位线 2000（无换行）：只有正常结束时它才是一条记录，用来关闭 `[1000,2000)`；读取故障时它只是未结束尾部。

读取器的关键实现就是"最后一批字节和结束原因同一次返回"——真实连接正是这样把缓冲区残留字节和故障一起交出来的：

```go
func (r *endWithReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		if r.end != nil {
			return 0, r.end // end 为 nil 时下面返回裸 io.EOF
		}
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos < len(r.data) {
		return n, nil
	}
	if r.end != nil {
		return n, r.end // 最后一批字节与故障同一次 Read 返回
	}
	return n, io.EOF     // 最后一批字节与裸 io.EOF 同一次 Read 返回
}
```

**方式一：正常结束（读取器返回裸 `io.EOF`）。** 无换行的最后一条水位线仍按完整的最后一条记录处理，两个窗口都关闭：

- `out`（窗口结果，恰好两行；区间即各自行的 `start`/`end`）：

```json
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":5}
{"key":"sensor-a","start":1000,"end":2000,"count":1,"sum":7}
```

- `lateLog`（迟到提示）：为空——本例没有严格低于当前水位线的事件。
- 返回错误：`nil`。`RunAggregate` 在正常结束时消费掉裸 `io.EOF`，调用方按通常的 `err == nil` 判断成功即可，不会收到 `io.EOF`。

**方式二：输入源在交付最后一批字节时报告读取故障。** 第 1～3 行的换行都已收到，全部生效；第 4 行水位线没有换行，属于未结束尾部，直接舍弃，不推进水位线。因此：

- `out`：只有第一个窗口一行，第二个窗口**不会因为调用返回了错误就补发**——窗口只由已生效的水位线关闭，结束（无论正常还是故障）都不补发未关闭窗口：

```json
{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":5}
```

- `lateLog`：为空（被舍弃的尾部既不是事件也不是水位线，不产生任何迟到提示）。
- 返回错误：输入源自己的错误原样返回，示例里是 `upstream connection reset by peer`；`errors.Is(err, errUpstream)` 为 `true`，而 `errors.As(err, *edgefleet.InputError)` 为 `false`——调用方据此知道这是**输入源的读取故障**，不能把它误报成"第 4 行 JSON 不合法"。第 4 行的 JSON 恰好是完整的也一样：没收到换行，它就不是记录。

**方式三：包装过的 `io.EOF` 仍然是读取故障。** 把上面的故障换成 `fmt.Errorf("gateway closed while delivering the last batch: %w", io.EOF)` 再跑一次：`errors.Is(err, io.EOF)` 虽然为 `true`，但它不是裸 `io.EOF`，输出与方式二完全相同——只有 `[0,1000)` 一行，返回错误为 `gateway closed while delivering the last batch: EOF`。自己实现 `io.Reader` 时，正常结束必须 `return io.EOF` 本身，不要返回它的包装副本。

### 故障同批交付的完整记录本身有格式错误时，记录错误优先

如果故障那批字节里、换行之前的某条完整记录本身格式就不对，则**优先返回该记录的错误**，而不是后知道的读取错误；定位按物理行号，**空行也占行号**，出错记录之后的记录和未结束尾部都不再处理。示例第 4 个场景使用如下输入（注意第 2 行是空行）：

```text
line 1: {"type":"event","key":"sensor-a","time":100,"value":5}   + "\n"
line 2:                                                           + "\n"   （空行）
line 3: not-json                                                  + "\n"   （完整但格式错误）
line 4: {"type":"watermark","time":1000}                          (无换行，未结束尾部)
```

读取器同样在最后一批字节上返回故障，但：

- 返回错误是 `line 3: invalid JSON record: invalid character 'o' in literal null (expecting 'u')`，可通过 `errors.As(err, *edgefleet.InputError)` 取到结构化的 `Line=3` 和 `Reason`；它**覆盖**了之后才获知的读取错误（此时 `errors.Is(err, errUpstream)` 为 `false`）。
- 第 3 行之前没有任何窗口被关闭，所以 `out` 为空；`lateLog` 也为空。第 4 行尾部和读取故障都不会再被处理或上报。

输出写出侧的失败规则与读取侧对称：窗口结果或迟到提示未能完整写出时，返回 `*edgefleet.OutputError`（可 `errors.Is` 到写出器自己的错误），触发它的物理行号、结果类别、key 与窗口区间或迟到事件时间都会标明，已写出的字节保留。特别注意：写出器**收下整条结果（JSON 加换行）之后仍返回自己的错误时，调用依然失败**——完整字节数不能抵消错误。完整规则、可运行示例与逐场景输出见下一节《示例：通过 Go 库接入输出流——窗口结果写出失败如何判定》。

以上规则均为现有产品行为，`edgefleet aggregate` 命令的用法、输入格式以及"输入结束时不补发未关闭窗口"的规则保持不变；库入口只是让调用方能够直接提供自己的 `io.Reader`/`io.Writer`，并按错误身份区分读取结束原因与写出失败原因。

## 示例：通过 Go 库接入输出流——窗口结果写出失败如何判定

上一节解决"输入是怎么结束的"，这一节解决**结果有没有真正送出去**。库入口仍然是 `edgefleet.RunAggregate`（滑动、分区变体同理）：

```go
func RunAggregate(r io.Reader, windowMillis int64, out io.Writer, lateLog io.Writer) error
```

`out`、`lateLog` 都是普通 `io.Writer`：真实接入时它们可能是网络连接、管道或日志管线客户端，对端可能在收下部分甚至全部字节后仍然报错。示例代码位于 [`examples/aggregate-write/main.go`](examples/aggregate-write/main.go)，输入用内存里的 `strings.Reader`、输出用内存里的自写 `io.Writer`，不涉及文件、网络与当前时间，可离线直接运行：

```bash
go run ./examples/aggregate-write
```

### 一条结果怎样才算"写出了"

每条输出都必须在**一次 `Write` 调用中完整落地**：窗口结果是"完整 JSON 对象 **加行尾换行**"（`{"key":...}\n`），迟到提示是完整一行。判定只有两条：

| `Write(p)` 的返回 | 含义 | `RunAggregate` 返回 |
| --- | --- | --- |
| `n == len(p)` 且 `err == nil` | 完整写出 | 继续处理 |
| `err != nil`（无论 `n` 是 0、部分还是 **`len(p)`**） | 写出端自己的故障 | `*edgefleet.OutputError`，其 `Err` 为写出器返回的**原始错误** |
| `err == nil` 但 `n < len(p)` | 内容没写全 | `*edgefleet.OutputError`，其 `Err` 为 `io.ErrShortWrite` |

两个容易踩空的点：

- **收下整条 JSON 和换行、同时返回自己的错误，仍然是写出失败。** 完整字节数不能抵消错误——字节已经到了对端的缓冲区，不等于对端承认了这次投递；调用方必须按返回错误处理，不能因为"行看起来完整"就当成功。
- **只收部分字节却返回 `nil`，报 `io.ErrShortWrite`。** 这是 `io.Writer` 契约里的短写：没有错误身份可以挂，库统一补上 `io.ErrShortWrite`，调用方用 `errors.Is(err, io.ErrShortWrite)` 识别。
- 两种情况可以叠加区分：收了**部分**字节、又返回**自己的错误**时，保留的是那个原始错误（`errors.Is` 能到它），而不是 `io.ErrShortWrite`——短写哨兵只在写出器返回空错误时使用。

失败统一包成 `*edgefleet.OutputError`，用 `errors.As` 取出：

| 字段 | 含义 |
| --- | --- |
| `Line` | 触发这批输出的**物理输入行号**（空行也占行号） |
| `Kind` | 结果类别：`"window result"`（窗口结果）或 `"late-event notice"`（迟到提示） |
| `Detail` | 窗口结果给出 `key "k" window [start,end)`；迟到提示给出迟到事件时间与判定所用水位线 |
| `Err` | 写出端的错误：写出器自己的错误（原样保留），或 `io.ErrShortWrite`；`errors.Is` 经 `Unwrap` 生效 |

错误字符串形如：

```text
line 6: window result not fully written (key "sensor-a" window [1000,2000)): result sink connection broken
```

### 失败之后：保留什么、不做什么

一次写出失败立即终止本次运行，规则与读取失败对称：

- 写出器**已经收下的字节原样保留**：此前完整写出（含换行）的结果行仍是有效窗口结果；失败那一次 `Write` 收下的字节——哪怕恰好是一整行——只是**物理残留，不是被承认的窗口结果**，调用方要靠 `*OutputError` 而不是靠"缓冲里有没有这行"来判定。
- 引擎**不会重发或重试**失败内容（不会补写缺失尾部），**不会继续输出同一水位线本批剩余的窗口**，也**不再读取或处理后续任何记录**；输入结束（无论正常 EOF 还是读取故障）**不会补发**仍未关闭的窗口。
- 迟到提示写出失败时规则相同，只是 `Kind` 为 `"late-event notice"`、`Detail` 标明迟到事件时间和水位线。

### 同一份输入：一条水位线关闭三个窗口

示例四个场景共用同一份逐行 JSON 输入（1000 毫秒固定窗口，单个 key `sensor-a`，共 **7 个物理行**；**第 3 行是空行**，空行被忽略但照占行号，所以触发写出的水位线在第 6 行；第 7 行无尾换行）：

```text
line 1: {"type":"event","key":"sensor-a","time":100,"value":1}    + "\n"
line 2: {"type":"event","key":"sensor-a","time":150,"value":2}    + "\n"
line 3: (blank line)                                              + "\n"
line 4: {"type":"event","key":"sensor-a","time":1100,"value":3}   + "\n"
line 5: {"type":"event","key":"sensor-a","time":2100,"value":4}   + "\n"
line 6: {"type":"watermark","time":3000}                          + "\n"
line 7: {"type":"event","key":"sensor-a","time":3100,"value":5}   (no newline)
```

窗口参数是 `RunAggregate(r, 1000, out, lateLog)`。第 1、2 行事件落在 `[0,1000)`（count=2、sum=3），第 4 行落在 `[1000,2000)`（count=1、sum=3），第 5 行落在 `[2000,3000)`（count=1、sum=4）。**第 6 行水位线 3000 在一次处理中按 end 升序连续关闭三个窗口**，每个窗口对应一次 `Write`：

```text
[0,1000)  ->  第 1 次 Write（成功后即被承认）
[1000,2000)  ->  第 2 次 Write（示例在这一次制造失败）
[2000,3000)  ->  第 3 次 Write（失败后不再发生）
```

第 7 行事件属于 `[3000,4000)`，之后再无水位线；即使在成功场景里，输入结束也不会补发它。示例的故障写出器只在**第 2 次 `Write`**（中间窗口 `[1000,2000)`）上按脚本行动，于是同一份输入就能对照"前面的结果成功写出、中间一条写出出问题、后面还有本可输出的窗口"。

### 场景一（对照）：写出端正常，三个窗口全部输出

`out` 是健康的缓冲写出器。返回错误为 `nil`，接收端恰好三行完整结果（每行都带换行），无残留片段，`lateLog` 为空：

```text
=== 1. control: a healthy bytes.Buffer writer ===
result lines fully written by EARLIER, successful calls (valid window results):
  {"key":"sensor-a","start":0,"end":1000,"count":2,"sum":3}
  {"key":"sensor-a","start":1000,"end":2000,"count":1,"sum":3}
  {"key":"sensor-a","start":2000,"end":3000,"count":1,"sum":4}
bytes the failing call itself left behind: (none — no call failed)
late-event notice stream (lateLog): (none)
returned error: <nil>
  err == nil -> run succeeded; every window the watermark closed was written
```

### 场景二：整条 JSON 与换行都已收下，写出器同时返回自己的错误

第 2 次 `Write` 收下全部 61 个字节（`{"key":...,"sum":3}\n`，长度恰好等于提交长度）却返回 `result sink connection broken`。调用仍然失败：错误是 `*edgefleet.OutputError`，`Err` 为写出器原始错误，`errors.Is(err, errSinkBroken)` 为 `true`，而 `errors.Is(err, io.ErrShortWrite)` 为 `false`。接收端的字节分成两部分呈现——**此前成功调用写出的 `[0,1000)` 一行是有效结果；失败那一次留下的一整行物理上存在，但不被承认**，不能当成 `[1000,2000)` 的窗口结果处理：

```text
=== 2. failing call offers/keeps the whole line (JSON + newline) and returns its own error ===
result lines fully written by EARLIER, successful calls (valid window results):
  {"key":"sensor-a","start":0,"end":1000,"count":2,"sum":3}
bytes the failing call itself left behind: "{\"key\":\"sensor-a\",\"start\":1000,\"end\":2000,\"count\":1,\"sum\":3}\n" (61 of 61 bytes offered) — the WHOLE result line including its newline is physically present, but the Write returned an error: it is retained yet NOT an acknowledged window result
late-event notice stream (lateLog): (none)
returned error: line 6: window result not fully written (key "sensor-a" window [1000,2000)): result sink connection broken
  errors.As(*edgefleet.OutputError) -> true
    triggering input line  : 6 (the blank physical line 3 kept its line number)
    result category (Kind) : "window result"
    record detail          : key "sensor-a" window [1000,2000)
    writer error (Err)     : result sink connection broken
  errors.Is(err, errSinkBroken)  -> true (the writer's own error exposed through Unwrap)
  errors.Is(err, io.ErrShortWrite) -> false
```

### 场景三：只收下 12 个字节的前缀，却返回空错误

第 2 次 `Write` 只保留前缀 `{"key":"sens`（61 字节中的 12 个）并返回 `nil`。这是短写：`Err` 为 `io.ErrShortWrite`，`errors.Is(err, io.ErrShortWrite)` 为 `true`。接收端在第一个完整行之后是一个**残留片段**，它不是合法 JSON、更不是窗口结果；同批第三个窗口 `[2000,3000)` 不会再写出：

```text
=== 3. failing call keeps only a 12-byte prefix and returns nil ===
result lines fully written by EARLIER, successful calls (valid window results):
  {"key":"sensor-a","start":0,"end":1000,"count":2,"sum":3}
bytes the failing call itself left behind: "{\"key\":\"sens" (12 of 61 bytes offered) — a residual fragment, NOT a valid window result
late-event notice stream (lateLog): (none)
returned error: line 6: window result not fully written (key "sensor-a" window [1000,2000)): short write
  errors.As(*edgefleet.OutputError) -> true
    triggering input line  : 6 (the blank physical line 3 kept its line number)
    result category (Kind) : "window result"
    record detail          : key "sensor-a" window [1000,2000)
    writer error (Err)     : short write
  errors.Is(err, errSinkBroken)  -> false (the writer's own error exposed through Unwrap)
  errors.Is(err, io.ErrShortWrite) -> true
```

### 场景四：收下部分前缀，同时返回自己的错误

第 2 次 `Write` 同样只保留 12 字节前缀，但这次返回 `result sink connection broken`。保留的是**写出器的原始错误**而不是短写哨兵：`errors.Is(err, errSinkBroken)` 为 `true`、`errors.Is(err, io.ErrShortWrite)` 为 `false`。接收端内容与场景三相同（一个完整行加一个残留片段），区别只在错误身份——这正是调用方区分"对端明确报错"与"对端违约式短写"的依据：

```text
=== 4. failing call keeps only a 12-byte prefix and returns its own error ===
result lines fully written by EARLIER, successful calls (valid window results):
  {"key":"sensor-a","start":0,"end":1000,"count":2,"sum":3}
bytes the failing call itself left behind: "{\"key\":\"sens" (12 of 61 bytes offered) — a residual fragment, NOT a valid window result
late-event notice stream (lateLog): (none)
returned error: line 6: window result not fully written (key "sensor-a" window [1000,2000)): result sink connection broken
  errors.As(*edgefleet.OutputError) -> true
    triggering input line  : 6 (the blank physical line 3 kept its line number)
    result category (Kind) : "window result"
    record detail          : key "sensor-a" window [1000,2000)
    writer error (Err)     : result sink connection broken
  errors.Is(err, errSinkBroken)  -> true (the writer's own error exposed through Unwrap)
  errors.Is(err, io.ErrShortWrite) -> false
```

### 把接收内容对应回水位线

三个失败场景的接收端可以直接对照第 6 行水位线本应关闭的三个窗口：

| 窗口 | 触发它的水位线 | 三个失败场景里的结局 |
| --- | --- | --- |
| `[0,1000)` | 第 6 行（本批第 1 次写出，先于故障） | **完整保留**，是唯一被承认的窗口结果行 |
| `[1000,2000)` | 第 6 行（本批第 2 次写出，即故障点） | 未被承认：场景二物理上留有一整行，场景三/四留有 12 字节片段；**均不是有效结果**，引擎不重发 |
| `[2000,3000)` | 第 6 行（本批第 3 次写出） | **完全没有输出**：故障后同批剩余窗口不再写出 |
| `[3000,4000)` | 本应由更晚的水位线关闭 | 第 7 行事件在故障后不再被读取；即使读到，输入结束也不补发未关闭窗口 |

接入方据此即可判定一次调用：`err == nil` 且收到的完整行覆盖本批全部窗口才算成功；拿到 `*edgefleet.OutputError` 时，用 `Line`/`Kind`/`Detail` 定位是哪条输入触发的哪个窗口结果，用 `errors.Is` 在写出端原始错误与 `io.ErrShortWrite` 之间区分原因，再按"完整行保留、残留丢弃、后续窗口缺失"来处理下游一致性。窗口结果与迟到提示的写出规则、错误类型在四个库入口（`RunAggregate`、`RunAggregateSliding`、`RunAggregatePartitioned`、`RunAggregatePartitionedSliding`）上完全一致。

## 技术方向

validator, node-monitoring, device-fleet, p2p-network, telemetry, devnet

## 运行要求

Go 1.26，仅使用标准库，全部行为可在本机 CPU 上离线复现。
