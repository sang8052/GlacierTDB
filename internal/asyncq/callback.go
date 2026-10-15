package asyncq

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"time"

	"glaciertdb/internal/logx"
)

// deliver 按通道分发回调。http 在 asyncq.go 主流程；此文件实现 tcp/amqp/kafka。
func (e *Engine) deliver(j *Job, payload map[string]any) {
	switch j.CallbackProto {
	case "", "http", "https":
		body, _ := json.Marshal(payload)
		e.callbackHTTP(j, body) // 原有 HTTP 路径
	case "tcp":
		e.callbackTCP(j, payload)
	case "amqp":
		e.callbackAMQP(j, payload)
	case "kafka":
		e.callbackKafka(j, payload)
	default:
		logx.Warnf("unknown callback proto %q (job %s)", j.CallbackProto, j.ID)
	}
}

// callbackTCP 客户端开端口，服务端 TCP 连接推送：首行 JSON 元信息，随后结果 JSONL 流，EOF 结束。
func (e *Engine) callbackTCP(j *Job, payload map[string]any) {
	addr := strings.TrimPrefix(strings.TrimPrefix(j.CallbackURL, "tcp://"), "tcp:")
	for attempt, backoff := range []time.Duration{0, time.Second, 5 * time.Second} {
		time.Sleep(backoff)
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			logx.Warnf("tcp callback dial %s failed (job %s, attempt %d): %v", addr, j.ID, attempt+1, err)
			continue
		}
		w := bufio.NewWriter(conn)
		meta, _ := json.Marshal(payload)
		w.Write(meta)
		w.WriteByte('\n')
		// 结果流：逐行转发结果文件（小结果直接内联 data 行）
		if j.State == StateDone {
			cols, rows, err := e.Fetch(j.ID, 0, 0)
			if err == nil {
				hdr, _ := json.Marshal(map[string]any{"columns": cols})
				w.Write(hdr)
				w.WriteByte('\n')
				for _, r := range rows {
					line, _ := json.Marshal(r)
					w.Write(line)
					w.WriteByte('\n')
				}
			}
		}
		w.Flush()
		conn.Close()
		return
	}
}

// callbackAMQP 发布到 RabbitMQ（URL 形如 amqp://user:pass@host:5672/）。
func (e *Engine) callbackAMQP(j *Job, payload map[string]any) {
	if amqpDisabled {
		logx.Warnf("amqp callback unavailable: amqp091-go not compiled in (job %s)", j.ID)
		return
	}
	amqpPublish(j, payload)
}

// callbackKafka 发布到 Kafka topic。
func (e *Engine) callbackKafka(j *Job, payload map[string]any) {
	if kafkaDisabled {
		logx.Warnf("kafka callback unavailable: kafka-go not compiled in (job %s)", j.ID)
		return
	}
	kafkaPublish(j, payload)
}

