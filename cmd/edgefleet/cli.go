package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

const defaultDataDir = "./edgefleet-data"

type commonFlags struct {
	dataDir string
	at      string
}

func bindCommon(fs *flag.FlagSet) *commonFlags {
	c := &commonFlags{}
	fs.StringVar(&c.dataDir, "data-dir", envOr("EDGEFLEET_DATA_DIR", defaultDataDir),
		"心跳数据目录")
	fs.StringVar(&c.at, "at", "", "接收/查询时间（RFC3339 带时区，如 2026-10-01T12:00:00+08:00），缺省为当前时间")
	return c
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func resolveAt(at string) (time.Time, error) {
	if at == "" {
		return time.Now(), nil
	}
	t, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return time.Time{}, fmt.Errorf("--at 必须是带时区的 RFC3339 时间 (如 2026-10-01T12:00:00+08:00): %w", err)
	}
	return t, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "错误: "+err.Error())
	os.Exit(1)
}

func printJSON(v any) {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fail(err)
	}
	os.Stdout.Write(out)
	fmt.Println()
}

// heartbeatRecordJSON is the CLI-facing record shape (all fields explicit).
type heartbeatRecordJSON = struct {
	NodeID      string `json:"node_id"`
	Seq         *int64 `json:"seq"`
	CollectedAt string `json:"collected_at"`
	Version     string `json:"version"`
	Height      *int64 `json:"height"`
	Missed      *int64 `json:"missed"`
}

type batchEnvelope struct {
	Heartbeats []json.RawMessage `json:"heartbeats"`
}

func runHeartbeatReceive(args []string) {
	fs := flag.NewFlagSet("heartbeat-receive", flag.ContinueOnError)
	c := bindCommon(fs)
	file := fs.String("file", "", "从文件读取 JSON（缺省或为 - 时从标准输入读取）")
	fs.Usage = func() {
		fmt.Print(`用法: edgefleet heartbeat-receive [--data-dir 目录] [--at 时间] [--file 文件]

接收一批节点心跳并整批持久化（断连期间缓存的心跳可在恢复后补传）。

输入格式（JSON，三种形态均可）:
  1) 心跳数组:                 [ {心跳}, {心跳}, ... ]
  2) 带包装的对象:             { "heartbeats": [ {心跳}, ... ] }
  3) 单条心跳对象:             {心跳}

每条心跳必须显式提供以下全部字段:
  node_id      字符串，节点标识，不能为空
  seq          大于 0 的整数（同一节点内可跳跃、可乱序，迟到记录仍保存）
  collected_at 采集时间，带时区的 RFC3339 文本（如 2026-10-01T12:00:00+08:00），
               不得晚于本次接收时间
  version      字符串，节点版本，不能为空
  height       区块高度，整数，允许 0 但不得为负
  missed       累计漏签数，整数，允许 0 但不得为负

处理规则:
  - 同一 node_id + seq 且各字段完全相同（collected_at 按同一时刻比较）记为重复；
  - 同一 node_id + seq 但内容不同记为冲突，整批拒绝、禁止覆盖；
  - 批次内重复/冲突同上；任一条非法或冲突，整批均不生效；
  - 成功返回新增数 inserted 与重复数 duplicated；返回成功即整批已 fsync 落盘。

数据保存位置:
  --data-dir 下的 heartbeat.log（仅追加、带 CRC、整批提交）；
  同目录 heartbeat.lock 保证多个命令进程可同时接收/查询而不产生重复或半批可见。

示例:
  echo '[{"node_id":"val-eu-1","seq":1,"collected_at":"2026-10-01T12:00:00+08:00",
           "version":"1.26.0","height":100,"missed":0}]' \
    | edgefleet heartbeat-receive --data-dir ./data
`)
	}
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	at, err := resolveAt(c.at)
	if err != nil {
		fail(err)
	}

	rawInput, err := readInput(*file)
	if err != nil {
		fail(err)
	}
	batch, err := parseBatch(rawInput)
	if err != nil {
		fail(err)
	}

	store, err := edgefleet.Open(c.dataDir)
	if err != nil {
		fail(err)
	}
	defer store.Close()

	inserted, duplicated, err := store.Receive(batch, at)
	if err != nil {
		// Keep prior data queryable; nothing from this batch was committed.
		fail(err)
	}

	printJSON(map[string]any{
		"ok":          true,
		"data_dir":    store.DataDir(),
		"received_at": at.Format(time.RFC3339Nano),
		"submitted":   len(batch),
		"inserted":    inserted,
		"duplicated":  duplicated,
	})
}

func readInput(file string) ([]byte, error) {
	if file == "" || file == "-" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("读取标准输入失败: %w", err)
		}
		return data, nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("读取输入文件失败: %w", err)
	}
	return data, nil
}

// parseBatch accepts a heartbeat array, an enveloped object, or one record.
func parseBatch(raw []byte) ([]edgefleet.Heartbeat, error) {
	trimmed := trimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("输入为空：需要 JSON 心跳数据")
	}
	var raws []json.RawMessage
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, fmt.Errorf("心跳数组 JSON 无效: %w", err)
		}
	} else {
		var env batchEnvelope
		if err := json.Unmarshal(trimmed, &env); err != nil {
			return nil, fmt.Errorf("输入 JSON 无效: %w", err)
		}
		if len(env.Heartbeats) > 0 {
			raws = env.Heartbeats
		} else {
			// A bare heartbeat object (the object has no heartbeats array).
			var probe heartbeatRecordJSON
			if err := json.Unmarshal(trimmed, &probe); err != nil {
				return nil, fmt.Errorf("输入 JSON 无效: %w", err)
			}
			raws = []json.RawMessage{json.RawMessage(trimmed)}
		}
	}
	if len(raws) == 0 {
		return nil, fmt.Errorf("批次为空：至少需要一条心跳")
	}
	batch := make([]edgefleet.Heartbeat, len(raws))
	for i, r := range raws {
		hb, err := edgefleet.UnmarshalHeartbeat(r)
		if err != nil {
			return nil, fmt.Errorf("第 %d 条记录非法: %w", i+1, err)
		}
		batch[i] = hb
	}
	return batch, nil
}

func trimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && isSpace(b[start]) {
		start++
	}
	for end > start && isSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func runHeartbeatQuery(args []string) {
	fs := flag.NewFlagSet("heartbeat-query", flag.ContinueOnError)
	c := bindCommon(fs)
	node := fs.String("node", "", "要查询的节点标识（必填）")
	expected := fs.String("expected-version", "", "期望版本（必填），与当前 Evaluate 规则一致")
	tolerated := fs.Int64("tolerated-missed", 0, "允许的累计漏签数，超过即异常（默认 0）")
	fs.Usage = func() {
		fmt.Print(`用法: edgefleet heartbeat-query --node 节点 --expected-version 版本
                          [--tolerated-missed 数] [--data-dir 目录] [--at 时间]

按节点当前遥测查询健康。当前遥测始终取该节点“最大序号”的心跳，而不是最后到达
的一条，因此刚补传的过期数据不会被误判为健康。

输出字段:
  seq / collected_at / version / height / missed  实际采用的那条心跳
  online     最新采集时间距查询时间不超过 60 秒为在线（恰好 60 秒仍在线）
  healthy    在线 且 版本等于期望版本 且 漏签数未超过允许值
  findings   异常原因（offline / version skew / missed duties above tolerance）
  reason     状态码: offline / query-before-telemetry / no-telemetry
  has_telemetry  节点从无心跳时为 false 且 findings 为 ["无遥测"]，
                 不会用零值冒充健康

错误:
  查询时间早于最新采集时间时明确报错（退出码非 0）。

数据来源: --data-dir 下的 heartbeat.log（与 heartbeat-receive 共用，可并发查询）。

示例:
  edgefleet heartbeat-query --data-dir ./data --node val-eu-1 \
    --expected-version 1.26.0 --tolerated-missed 1
`)
	}
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *node == "" {
		fail(fmt.Errorf("--node 为必填项"))
	}
	if *expected == "" {
		fail(fmt.Errorf("--expected-version 为必填项"))
	}
	if *tolerated < 0 {
		fail(fmt.Errorf("--tolerated-missed 不得为负, 得到 %d", *tolerated))
	}
	at, err := resolveAt(c.at)
	if err != nil {
		fail(err)
	}

	store, err := edgefleet.Open(c.dataDir)
	if err != nil {
		fail(err)
	}
	defer store.Close()

	status, err := store.Health(*node, at, *expected, *tolerated)
	printJSON(status.JSON())
	if err != nil {
		fail(err)
	}
}

func runHeartbeatHistory(args []string) {
	fs := flag.NewFlagSet("heartbeat-history", flag.ContinueOnError)
	c := bindCommon(fs)
	node := fs.String("node", "", "要查询的节点标识（必填）")
	fs.Usage = func() {
		fmt.Print(`用法: edgefleet heartbeat-history --node 节点 [--data-dir 目录]

返回某节点的全部历史心跳，按 seq 升序排列、不重复；包含每条记录的接收时间
received_at，可用于核对断连补传顺序。平台重启后结果保持一致。

数据来源: --data-dir 下的 heartbeat.log（与接收命令共用，可并发查询）。
`)
	}
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *node == "" {
		fail(fmt.Errorf("--node 为必填项"))
	}

	store, err := edgefleet.Open(c.dataDir)
	if err != nil {
		fail(err)
	}
	defer store.Close()

	hbs := store.History(*node)
	records := make([]map[string]any, 0, len(hbs))
	for _, hb := range hbs {
		data, _ := edgefleet.MarshalHeartbeat(hb)
		var rec map[string]any
		_ = json.Unmarshal(data, &rec)
		records = append(records, rec)
	}
	printJSON(map[string]any{
		"node_id":    *node,
		"data_dir":   store.DataDir(),
		"count":      len(records),
		"heartbeats": records,
	})
}
