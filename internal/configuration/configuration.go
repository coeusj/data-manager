package configuration

type KafkaConfig struct {
	Brokers string `json:"brokers"`
	Topic   string `json:"topic"`
	GroupID string `json:"groupId"`
}

type RedisConfig struct {
	Address        string `json:"address"`
	Namespace      string `json:"namespace"`
	DataTTLSeconds uint   `json:"dataTTLSeconds"`
	DBIndex        int    `json:"db"`
}
