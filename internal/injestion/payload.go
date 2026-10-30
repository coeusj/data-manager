package injestion

type Payload[K any, V any] struct {
	HashKey   [32]byte
	Key       *K
	Value     *V
	IsDelete  bool
	Partition int32
	Offset    int64
}
