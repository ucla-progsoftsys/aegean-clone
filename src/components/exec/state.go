package exec

type State struct {
	KVStore  map[string]string
	SeqNum   int
	PrevHash string
	Verified bool
}
