package kinesis

import "github.com/elastic/beats/libbeat/outputs/codec"

type Config struct {
	AWSRegion          string `config:"aws_region"`
	AWSAccessKeyID     string `config:"aws_access_key_id"`
	AWSSecretAccessKey string `config:"aws_secret_access_key"`
	StreamName         string `config:"stream_name"`
	PartitionKey       string `config:"partition_key"`

	BulkMaxSize int          `config:"bulk_max_size"`
	MaxRetries  int          `config:"max_retries"`
	Codec       codec.Config `config:"codec"`
}

func newConfig() *Config {
	return &Config{}
}
