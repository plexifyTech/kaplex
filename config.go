package kaplex

var (
	config *KafkaConfig
)

type KafkaConfig struct {
	Url           string `yaml:"url" env:"KAFKA_URL"`
	ConsumerGroup string `yaml:"consumerGroup" env:"KAFKA_CONSUMER_GROUP"`
}

func Init(c *KafkaConfig) {
	config = c
}
