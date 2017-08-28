package kinesis

import (
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
)

var debugf = logp.MakeDebug("kinesis")

func init() {
	outputs.RegisterType("kinesis", makeKinesis)
}

func makeKinesis(
	beat beat.Info,
	stats *outputs.Stats,
	cfg *common.Config,
) (outputs.Group, error) {

	debugf("initialize kinesis output")

	config := newConfig()
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}

	codec, err := codec.CreateEncoder(config.Codec)
	if err != nil {
		return outputs.Fail(err)
	}

	bulkMaxSize := config.BulkMaxSize
	if bulkMaxSize > 100 || bulkMaxSize < 0 {
		bulkMaxSize = 100
	}

	client := newClient(stats, beat.Beat, codec, config)

	retry := 0
	if config.MaxRetries < 0 {
		retry = -1
	}

	return outputs.Success(bulkMaxSize, retry, client)
}
