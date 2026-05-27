package kvstore

// Simple K-V store
type KVStore struct {
	store map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string]string),
	}
}

func (kv *KVStore) Put(key string, value string) {
	kv.store[key] = value
}

func (kv *KVStore) Append(key string, value string) {
	kv.store[key] += value
}

func (kv *KVStore) Get(key string) (string, bool) {
	value, ok := kv.store[key]
	return value, ok
}
