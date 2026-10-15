package asyncq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"

	"glaciertdb/internal/logx"
)

const amqpDisabled = false
const kafkaDisabled = false

func buildMessage(j *Job, payload map[string]any) []byte {
	body, err := json.Marshal(payload)
	if err != nil {
		return []byte("{}")
	}
	return body
}

// amqpPublish 发布到 RabbitMQ：URL=CallbackURL（amqp://user:pass@host:5672/），队列=CallbackOpt（默认 glacier_jobs）。
func amqpPublish(j *Job, payload map[string]any) {
	queue := j.CallbackOpt
	if queue == "" {
		queue = "glacier_jobs"
	}
	for attempt, backoff := range []time.Duration{0, time.Second, 5 * time.Second} {
		time.Sleep(backoff)
		conn, err := amqp091.Dial(j.CallbackURL)
		if err != nil {
			logx.Warnf("amqp dial failed (job %s, attempt %d): %v", j.ID, attempt+1, err)
			continue
		}
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			logx.Warnf("amqp channel: %v (job %s)", err, j.ID)
			continue
		}
		q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
		if err != nil {
			ch.Close(); conn.Close()
			logx.Warnf("amqp declare: %v (job %s)", err, j.ID)
			continue
		}
		err = ch.Publish("", q.Name, false, false, amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			Timestamp:    time.Now(),
			Body:         buildMessage(j, payload),
		})
		ch.Close()
		conn.Close()
		if err == nil {
			return
		}
		logx.Warnf("amqp publish failed (job %s): %v", j.ID, err)
	}
}

// kafkaPublish 发布到 Kafka：broker=CallbackURL（host:9092[,host2]），topic=CallbackOpt（默认 glacier_jobs）。
func kafkaPublish(j *Job, payload map[string]any) {
	topic := j.CallbackOpt
	if topic == "" {
		topic = "glacier_jobs"
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(j.CallbackURL),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 500 * time.Millisecond,
	}
	defer w.Close()
	msg := kafka.Message{
		Key:   []byte(j.ID),
		Value: buildMessage(j, payload),
		Time:  time.Now(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msg); err != nil {
		logx.Warnf("kafka publish failed (job %s): %v", j.ID, err)
	}
}
