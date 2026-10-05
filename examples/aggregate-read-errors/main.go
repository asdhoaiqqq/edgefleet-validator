// 本程序可在本机离线运行，演示通过 Go 库接入聚合输入流时，如何区分
// “输入正常结束”和“输入读取失败”：
//
//	go run ./examples/aggregate-read-errors
//
// 它使用 edgefleet.RunAggregate、1000 毫秒固定窗口，输入与输出全部在内存中，
// 不访问网络、不读取文件、不依赖当前时间。
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

// errUpstreamRead 代表输入源（例如 net.Conn、管道或设备驱动）自身报告的读取
// 故障。真实程序里它可以是任何携带自己原因的错误；RunAggregate 会把它原样
// 交还调用方，因此这里用 errors.Is 即可识别输入源给出的原因。
var errUpstreamRead = errors.New("simulated upstream device read failure")

// failingReader 一次性交付 data 的全部字节：最后一次 Read 在交付剩余字节的
// 同时返回 failErr，模拟输入源在交出最后一批字节时报告故障。它也可以返回
// 包装过的 io.EOF——本程序场景三用它证明“包装的 EOF 仍是故障”。
type failingReader struct {
	data    []byte
	pos     int
	failErr error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.failErr
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, r.failErr // 字节与故障在同一批交付
	}
	return n, nil
}

// commonInput 是四种说明围绕的同一份输入（4 条物理记录，最后一条没有换行）：
//
//	第 1 行：事件 key=sensor-a time=100 value=5（带换行）
//	第 2 行：水位线 time=1000（带换行）——关闭窗口 [0,1000)
//	第 3 行：事件 key=sensor-a time=1100 value=7（带换行）
//	第 4 行：水位线 time=2000（无换行）——只有正常结束时才会处理
const commonInput = "" +
	`{"type":"event","key":"sensor-a","time":100,"value":5}` + "\n" +
	`{"type":"watermark","time":1000}` + "\n" +
	`{"type":"event","key":"sensor-a","time":1100,"value":7}` + "\n" +
	`{"type":"watermark","time":2000}`

func main() {
	runCleanEOF()
	runReadFailure()
	runWrappedEOF()
	runRecordErrorInFailingBatch()
}

// dump 汇总一次 RunAggregate 调用拿到的全部结果。
func dump(title string, r io.Reader) {
	var out, lateLog bytes.Buffer
	err := edgefleet.RunAggregate(r, 1000, &out, &lateLog)

	fmt.Printf("=== %s ===\n", title)
	fmt.Print("窗口结果 out:\n")
	fmt.Print(indentOrEmpty(out.String()))
	fmt.Printf("迟到提示 lateLog: %s\n", quotedOrEmpty(lateLog.String()))
	fmt.Printf("返回错误 err: %v\n", errOrNil(err))

	// 识别一：读取器自己的错误会原样返回，errors.Is 能找到输入源报告的原因。
	fmt.Printf("errors.Is(err, errUpstreamRead) = %v\n", errors.Is(err, errUpstreamRead))
	// 识别二：绝不能用 errors.Is(err, io.EOF) 判断成功。包装过的 io.EOF 仍是
	// 读取故障（见场景三）；只有读取器直接返回裸 io.EOF 才是正常结束，而那种
	// 情况下 RunAggregate 返回的是 nil。
	fmt.Printf("errors.Is(err, io.EOF)         = %v\n", errors.Is(err, io.EOF))
	// 识别三：读取故障不会被误报成某一行 JSON 不合法。
	var in *edgefleet.InputError
	fmt.Printf("errors.As(*edgefleet.InputError) = %v", errors.As(err, &in))
	if in != nil {
		fmt.Printf("（物理行号 %d，原因 %q）", in.Line, in.Reason)
	}
	fmt.Print("\n\n")
}

func runCleanEOF() {
	// strings.Reader 在读完后返回裸 io.EOF。最后一条没有换行的水位线
	// 2000 仍会作为完整的最终记录处理，于是第二个窗口也被关闭。
	dump("场景一：正常结束（读取器直接返回裸 io.EOF）", strings.NewReader(commonInput))
}

func runReadFailure() {
	// 同一份字节，但最后一次 Read 在交付字节的同时报告输入源故障。
	// 已经收到换行的记录照常生效；没有换行的水位线 2000 属于未结束尾部，
	// 必须舍弃：第二个窗口不会因为返回错误而补发。
	dump("场景二：最后一批字节与读取故障同时到达", &failingReader{
		data:    []byte(commonInput),
		failErr: errUpstreamRead,
	})
}

func runWrappedEOF() {
	// 包装过的 io.EOF：errors.Is 能沿 Unwrap 链找到 io.EOF，但它不是裸
	// io.EOF，库按读取故障处理——未结束尾部舍弃，包装错误原样返回。
	wrapped := fmt.Errorf("device stream closed abruptly: %w", io.EOF)
	dump("场景三：包装过的 io.EOF 仍按读取故障处理", &failingReader{
		data:    []byte(commonInput),
		failErr: wrapped,
	})
}

func runRecordErrorInFailingBatch() {
	// 追加一条说明：如果与故障同批交付的完整记录本身有格式错误，记录错误
	// 优先于读取故障，按包含空行在内的物理行号定位；其后的记录和未结束
	// 尾部都不再处理。
	input := strings.Join([]string{
		`{"type":"event","key":"sensor-a","time":100,"value":5}`, // 第 1 行
		``,                                 // 第 2 行：空行，仍占物理行号
		`{"type":"watermark","time":1000}`, // 第 3 行：带换行，关闭 [0,1000)
		`{"type":"event",}`,                // 第 4 行：带换行的完整行，但 JSON 非法
		`{"type":"watermark","time":2000}`, // 第 5 行：无换行的未结束尾部
	}, "\n")
	dump("场景四：同批完整记录本身格式错误，记录错误优先", &failingReader{
		data:    []byte(input),
		failErr: errUpstreamRead,
	})
}

func indentOrEmpty(s string) string {
	if s == "" {
		return "（空）\n"
	}
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("  " + line + "\n")
	}
	return b.String()
}

func quotedOrEmpty(s string) string {
	if s == "" {
		return "（空）"
	}
	return fmt.Sprintf("%q", s)
}

func errOrNil(err error) any {
	if err == nil {
		return "<nil>"
	}
	return err
}
