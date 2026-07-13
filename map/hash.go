package fmap

const (
	defaultShardCount   = 64
	minFastMapCapacity  = 8
	fastMapLoadPercent  = 75
	fastMapRehashFactor = 2
)

type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

func HashInteger[K Integer](key K) uint64 {
	return Mix64(uint64(key))
}

func HashInt64(key int64) uint64 {
	return HashInteger(key)
}

func HashUint64(key uint64) uint64 {
	return HashInteger(key)
}

func HashString(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return Mix64(h)
}

func Mix64(v uint64) uint64 {
	v = (v ^ (v >> 30)) * 0xbf58476d1ce4e5b9
	v = (v ^ (v >> 27)) * 0x94d049bb133111eb
	return v ^ (v >> 31)
}

func nextPowerOfTwo(v int) int {
	if v <= 1 {
		return 1
	}
	n := 1
	for n < v {
		n <<= 1
	}
	return n
}

func normalizeShardCount(shardCount int) int {
	if shardCount <= 0 {
		shardCount = defaultShardCount
	}
	return nextPowerOfTwo(shardCount)
}

func fastMapCapacityForHint(capHint int) int {
	if capHint < 0 {
		capHint = 0
	}
	capacity := minFastMapCapacity
	for capacity*fastMapLoadPercent/100 < capHint {
		capacity <<= 1
	}
	return capacity
}
