package api

import (
	"crypto/rand"
	"time"
)

const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewULID generates a ULID (Crockford base32, 26 chars):
// 48-bit millisecond timestamp + 80 bits of randomness.
func NewULID() string {
	var b [16]byte
	ms := uint64(time.Now().UTC().UnixMilli())
	// 48-bit timestamp in the top 6 bytes (b[0] is most significant).
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	// 80 bits of randomness in the last 10 bytes.
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < 16; i++ {
			b[i] = byte(ms >> (i % 8))
		}
	}
	return encodeULID(b)
}

// encodeULID encodes 128 bits as 26 Crockford base32 chars.
func encodeULID(b [16]byte) string {
	var out [26]byte
	for i := 0; i < 26; i++ {
		var v byte
		for j := 0; j < 5; j++ {
			pos := 5*i + j // bit position from the MSB (0-based)
			if pos > 127 {
				break
			}
			byteIdx := pos / 8
			bitIdx := 7 - (pos % 8)
			if b[byteIdx]&(1<<bitIdx) != 0 {
				v |= 1 << uint(4-j)
			}
		}
		out[i] = ulidAlphabet[v]
	}
	return string(out[:])
}
