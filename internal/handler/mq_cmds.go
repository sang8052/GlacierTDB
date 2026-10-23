package handler

import (
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/meta"
)

// handleMQAdmin 消息队列管理命令（v1.3.0，USE GlacierTDB 管理面）：
//
//	SHOW QUEUES     —— 队列深度 / 消费者数 / 入出速率
//	SHOW EXCHANGES  —— 交换机类型 / 绑定数
func (h *Handler) handleMQAdmin(q string) (*mysql.Result, bool, error) {
	toks := lexSQL(q)
	if len(toks) < 2 {
		return nil, false, nil
	}
	up := func(i int) string {
		if i < len(toks) {
			return strings.ToUpper(toks[i])
		}
		return ""
	}
	if up(0) != "SHOW" {
		return nil, false, nil
	}
	switch up(1) {
	case "QUEUES":
		if h.mqstore == nil {
			return nil, true, mysql.NewError(1105, "MQ module is disabled (set mq.enable: true)")
		}
		rows := h.mqQueueRows()
		res, err := meta.SimpleRS(mqQueueCols, rows)
		return res, true, err
	case "EXCHANGES":
		if h.mqstore == nil {
			return nil, true, mysql.NewError(1105, "MQ module is disabled (set mq.enable: true)")
		}
		rows := h.mqExchangeRows()
		res, err := meta.SimpleRS(mqExchangeCols, rows)
		return res, true, err
	}
	return nil, false, nil
}

var mqQueueCols = []string{"namespace", "queue", "ready", "unacked", "consumers",
	"published", "delivered", "acked", "durable"}

var mqExchangeCols = []string{"namespace", "exchange", "type", "durable", "bindings"}

// mqQueueRows SHOW QUEUES / mq_queues 行集。
func (h *Handler) mqQueueRows() [][]any {
	var rows [][]any
	for _, q := range h.mqstore.Queues("") {
		rows = append(rows, []any{q.NS, q.Name, q.Ready, q.Unacked, int64(q.Consumers),
			q.Published, q.Delivered, q.Acked, b2s(q.Durable)})
	}
	return rows
}

// mqExchangeRows SHOW EXCHANGES / mq_exchanges 行集。
func (h *Handler) mqExchangeRows() [][]any {
	var rows [][]any
	for _, e := range h.mqstore.Exchanges("") {
		rows = append(rows, []any{e.NS, e.Name, e.Type, b2s(e.Durable), int64(e.Bindings)})
	}
	return rows
}
