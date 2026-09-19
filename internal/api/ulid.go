package api

import (
	"crypto/rand"
	//"encoding/binary"
	"sync"
	"time"
)

const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var (
	ulidMu sync.Mutex

	// Последний timestamp в миллисекундах.
	lastULIDMS uint64

	// Последние 80 бит entropy.
	lastULIDEntropy [10]byte
)

// NewULID generates a monotonic ULID.
//
// ULID format:
//   - 48-bit millisecond timestamp
//   - 80-bit entropy
//
// For multiple IDs generated within the same millisecond, entropy is
// incremented, which makes the resulting ULIDs lexicographically increasing.
func NewULID() string {
	ulidMu.Lock()
	defer ulidMu.Unlock()

	ms := uint64(time.Now().UnixMilli())

	var entropy [10]byte

	switch {
	case ms > lastULIDMS:
		if _, err := rand.Read(entropy[:]); err != nil {
			// Криптографическая случайность необходима для нормальной
			// работы генератора. Не продолжаем с предсказуемыми данными.
			panic("api: crypto/rand failed: " + err.Error())
		}

		lastULIDMS = ms
		lastULIDEntropy = entropy

	case ms == lastULIDMS:
		if !incrementEntropy(&lastULIDEntropy) {
			// За одну миллисекунду невозможно выдать больше 2^80
			// монотонных значений.
			panic("api: ULID entropy overflow")
		}

	case ms < lastULIDMS:
		// Системные часы пошли назад. Используем предыдущий timestamp,
		// иначе лексикографический порядок ULID будет нарушен.
		ms = lastULIDMS

		if !incrementEntropy(&lastULIDEntropy) {
			panic("api: ULID entropy overflow")
		}
	}

	var b [16]byte

	// 48-bit timestamp, big-endian.
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	// 80-bit entropy.
	copy(b[6:], lastULIDEntropy[:])

	return encodeULID(b)
}

// incrementEntropy increments a big-endian 80-bit integer.
//
// Returns false on overflow.
func incrementEntropy(entropy *[10]byte) bool {
	for i := len(entropy) - 1; i >= 0; i-- {
		if entropy[i] != 0xff {
			entropy[i]++
			return true
		}

		entropy[i] = 0
	}

	return false
}

// encodeULID encodes 128 bits as 26 Crockford base32 chars.
func encodeULID(b [16]byte) string {
	var out [26]byte

	for i := 0; i < 26; i++ {
		var value byte

		for j := 0; j < 5; j++ {
			pos := 5*i + j

			if pos > 127 {
				break
			}

			byteIndex := pos / 8
			bitIndex := 7 - (pos % 8)

			if b[byteIndex]&(1<<uint(bitIndex)) != 0 {
				value |= 1 << uint(4-j)
			}
		}

		out[i] = ulidAlphabet[value]
	}

	return string(out[:])
}
