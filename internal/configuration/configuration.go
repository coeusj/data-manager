package configuration

type KafkaConfig struct {
	Brokers string `json:"brokers"`
	Topic   string `json:"topic"`
	GroupId string `json:"groupId"`
}

type RedisConfig struct {
	Address        string `json:"address"`
	NameSpace      string `json:"namespace"`
	DataTTLSeconds uint   `json:"dataTTLSeconds"`
}
