package kinesis

import (
	"fmt"

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

func (c *client) putRecords(records []*kinesis.PutRecordsRequestEntry) error {
	if len(records) == 0 {
		return nil
	}

	for idx, r := range records {
		r.PartitionKey = aws.String(fmt.Sprintf("%s-%d", c.partitionKey, idx%200))
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

func (c *client) makeRecord(event *publisher.Event) (*kinesis.PutRecordsRequestEntry, error) {
	serializedEvent, err := c.codec.Encode(c.beat.Beat, &event.Content)
	if err != nil {
		return nil, err
	}
	return &kinesis.PutRecordsRequestEntry{
		Data: serializedEvent,
	}, nil
}

func (c *client) Publish(batch publisher.Batch) error {
	events := batch.Events()
	c.stats.NewBatch(len(events))

	var records []*kinesis.PutRecordsRequestEntry
	dropped := 0
	for i := range events {
		event := &events[i]
		record, err := c.makeRecord(event)
		if err != nil {
			if event.Guaranteed() {
				logp.Critical("Failed to serialize the event: %v", err)
			} else {
				logp.Warn("Failed to serialize the event: %v", err)
			}
			dropped++
			continue
		}
		records = append(records, record)
	}

	eventCount := len(records)
	err := c.putRecords(records)

	if err == nil {
		batch.ACK()
		c.stats.Acked(eventCount)
		c.stats.Dropped(dropped)
	} else {
		logp.Err("send event faild: %d - errors: %v", eventCount, err.Error())
		c.stats.Failed(eventCount)
	}

	return err
}

func (c *client) Close() error {
	return nil
}
