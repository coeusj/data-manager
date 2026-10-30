package injestion

type Payload[K any, V any] struct {
	ID        string
	Key       *K
	Value     *V
	IsDelete  bool
	Partition int32
	Offset    int64
}
