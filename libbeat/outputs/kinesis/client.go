package kinesis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/publisher"
)

type client struct {
	stats *outputs.Stats
	index string
	codec codec.Codec
	beat  beat.Info

	*kinesis.Kinesis
	streanName   string
	partitionKey string
}

type Records []*kinesis.PutRecordsRequestEntry

func newClient(stats *outputs.Stats, index string, codec codec.Codec, cfg *Config) *client {
	ci := &client{
		stats:        stats,
		index:        index,
		codec:        codec,
		streanName:   cfg.StreamName,
		partitionKey: cfg.PartitionKey,
	}
	sess, err := session.NewSession()
	if err != nil {
		panic(err.Error())
	}
	if len(cfg.AWSRegion) > 0 {
		sess.Config.Region = aws.String(cfg.AWSRegion)
	}
	ci.Kinesis = kinesis.New(sess)
	return ci
}

func (c *client) putRecord(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	_, err := c.PutRecord(&kinesis.PutRecordInput{
		StreamName:   aws.String(c.streanName),
		PartitionKey: aws.String(c.partitionKey),
		Data:         data,
	})
	return err
}

// 批量发送消息时候，发现消息会乱,大概问题在 拼接 records 时候,传入 这个方法后才乱
func (c *client) putRecords(records []*kinesis.PutRecordsRequestEntry) error {
	if len(records) == 0 {
		return nil
	}

	out, err := c.PutRecords(&kinesis.PutRecordsInput{
		StreamName: aws.String(c.streanName),
		Records:    records,
	})
	if err != nil {
		return err
	}

	if aws.Int64Value(out.FailedRecordCount) > 0 {
		var retryRecords []*kinesis.PutRecordsRequestEntry
		for idx, r := range out.Records {
			if r.ErrorCode != nil {
				retryRecords = append(retryRecords, records[idx])
			}
		}
		logp.Warn("Retry Failed Record: %d", len(retryRecords))
		return c.putRecords(retryRecords)
	}
	return nil
}

func (c *client) Close() error {
	return nil
}

// 批量发送版本
func (c *client) Publish(batch publisher.Batch) error {
	defer batch.ACK()

	events := batch.Events()
	c.stats.NewBatch(len(events))

	dropped := 0
	var records Records
	for i := range events {
		event := &events[i]
		if record, err := c.makeRecord(event); err == nil {
			records = append(records, record)
		} else {
			dropped++
		}
	}
	
	err := c.putRecords(records)
	if err == nil {
		c.stats.Acked(len(events) - dropped)
	} else {
		c.stats.Failed(len(events))
	}

	records = records[:0]

	c.stats.Dropped(dropped)
	return err
}

func (c *client) makeRecord(event *publisher.Event) (*kinesis.PutRecordsRequestEntry, error) {
	serializedEvent, err := c.codec.Encode(c.beat.Beat, &event.Content)
	if err != nil {
		if event.Guaranteed() {
			logp.Critical("Failed to serialize the event: %v", err)
		} else {
			logp.Warn("Failed to serialize the event: %v", err)
		}
		return nil, err
	}

	buf := make([]byte, len(serializedEvent))
	copy(buf, serializedEvent)
	return &kinesis.PutRecordsRequestEntry{
		Data:         buf,
		PartitionKey: aws.String(c.partitionKey),
	}, nil
}
