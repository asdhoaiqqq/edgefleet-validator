// Command edgefleet is the 验证者与边缘节点机群管理平台 entry point.
package main

import (
	"fmt"
	"os"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

func main() {
	command := "demo"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	switch command {
	case "demo":
		runDemo()
	case "version":
		fmt.Println("edgefleet 0.1.0")
	case "heartbeat-receive":
		runHeartbeatReceive(os.Args[2:])
	case "heartbeat-query":
		runHeartbeatQuery(os.Args[2:])
	case "heartbeat-history":
		runHeartbeatHistory(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", command)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Print(`edgefleet — 验证者与边缘节点机群管理平台

用法:
  edgefleet <command> [flags]

现有命令（行为保持不变）:
  demo                         以内存示例节点演示 Evaluate/Plan
  version                      打印版本号

真实心跳命令（本机离线运行，数据保存在本地数据目录）:
  heartbeat-receive            接收并持久保存一批心跳（支持断连补传）
  heartbeat-query              按节点当前遥测（最大序号）查询健康状态
  heartbeat-history            按节点查询全部历史心跳（序号升序、不重复）

通用标志:
  --data-dir <目录>            心跳数据目录（默认 ./edgefleet-data，
                               也可用环境变量 EDGEFLEET_DATA_DIR 指定）
  --at <RFC3339时间>           指定接收/查询时间（带时区，如
                               2026-10-01T12:00:00+08:00）；
                               不指定时使用当前时间
  -h, --help                   查看该命令的详细帮助

数据保存位置:
  <data-dir>/heartbeat.log     仅追加、带 CRC、整批提交的心跳日志（fsync 落盘）
  <data-dir>/heartbeat.lock    多进程并发互斥锁文件

运行示例:
  go run ./cmd/edgefleet heartbeat-receive -h
  go run ./cmd/edgefleet heartbeat-query    -h
  go run ./cmd/edgefleet heartbeat-history  -h
`)
}

func runDemo() {
	nodes := []edgefleet.Node{
		{ID: "val-eu-1", Region: "eu", Version: "1.26.0", Online: true},
		{ID: "val-us-1", Region: "us", Version: "1.25.4", Online: true, Missed: 3},
		{ID: "edge-ap-1", Region: "ap", Version: "1.26.0", Online: false},
	}
	unhealthy := 0
	for _, node := range nodes {
		health := edgefleet.Evaluate(node, "1.26.0", 1)
		if !health.Healthy {
			unhealthy++
		}
		fmt.Printf("node=%s healthy=%v findings=%v\n", health.Node, health.Healthy, health.Findings)
	}
	fmt.Println("rollout order:", edgefleet.Plan(nodes, "1.26.0"))
	fmt.Printf("summary: %d of %d nodes need attention\n", unhealthy, len(nodes))
}
