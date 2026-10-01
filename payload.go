package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type patch struct {
	offset, size int
	kind         string
}
type payload struct {
	body    []byte
	patches []patch
	state   uint64
}

func compilePayload(text []byte, dynamic bool) (*payload, error) {
	p := &payload{body: make([]byte, 0, len(text)), state: 1}
	if !dynamic {
		p.body = append(p.body, text...)
		return p, nil
	}
	s := string(text)
	for len(s) > 0 {
		i := strings.Index(s, "{{")
		if i < 0 {
			p.body = append(p.body, s...)
			break
		}
		p.body = append(p.body, s[:i]...)
		s = s[i+2:]
		end := strings.Index(s, "}}")
		if end < 0 {
			return nil, fmt.Errorf("unclosed template placeholder")
		}
		token := strings.TrimSpace(s[:end])
		s = s[end+2:]
		size := 0
		switch token {
		case "uuid":
			size = 36
		case "timestamp":
			size = 30
		case "sequence":
			size = 20
		default:
			if strings.HasPrefix(token, "random:") {
				n, err := strconv.Atoi(strings.TrimPrefix(token, "random:"))
				if err != nil || n < 1 || n > 1<<20 {
					return nil, fmt.Errorf("random placeholder size must be 1..1048576")
				}
				size = n
			} else {
				return nil, fmt.Errorf("unknown template placeholder")
			}
		}
		if len(p.body)+size > 16<<20 {
			return nil, fmt.Errorf("expanded template exceeds 16 MiB")
		}
		if len(p.patches) >= 65535 {
			return nil, fmt.Errorf("too many template placeholders")
		}
		p.patches = append(p.patches, patch{len(p.body), size, token})
		for j := 0; j < size; j++ {
			p.body = append(p.body, '0')
		}
	}
	return p, nil
}
func (p *payload) clone(worker int) *payload {
	return &payload{body: append([]byte(nil), p.body...), patches: p.patches, state: uint64(worker+1) * 0x9e3779b97f4a7c15}
}
func (p *payload) update(prefix [8]byte, seq uint64, _ int) {
	var stamp [40]byte
	timestamp := time.Now().UTC().AppendFormat(stamp[:0], "2006-01-02T15:04:05.000000000Z")
	for i, patch := range p.patches {
		b := p.body[patch.offset : patch.offset+patch.size]
		switch patch.kind {
		case "timestamp":
			copy(b, timestamp)
		case "sequence":
			n := seq
			for j := len(b) - 1; j >= 0; j-- {
				b[j] = byte(n%10) + '0'
				n /= 10
			}
		case "uuid":
			var raw [16]byte
			copy(raw[:8], prefix[:])
			n := seq*65536 + uint64(i)
			for j := 15; j >= 8; j-- {
				raw[j] = byte(n)
				n >>= 8
			}
			raw[6] = (raw[6] & 15) | 64
			raw[8] = (raw[8] & 63) | 128
			var out [32]byte
			hex.Encode(out[:], raw[:])
			copy(b[:8], out[:8])
			b[8] = '-'
			copy(b[9:13], out[8:12])
			b[13] = '-'
			copy(b[14:18], out[12:16])
			b[18] = '-'
			copy(b[19:23], out[16:20])
			b[23] = '-'
			copy(b[24:], out[20:])
		default:
			const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
			for j := range b {
				p.state ^= p.state << 13
				p.state ^= p.state >> 7
				p.state ^= p.state << 17
				b[j] = alphabet[p.state&63]
			}
		}
	}
}
func runPrefix(id string) ([8]byte, error) {
	var p [8]byte
	b, err := hex.DecodeString(id)
	if err != nil || len(b) != 8 {
		return p, fmt.Errorf("invalid run ID")
	}
	copy(p[:], b)
	return p, nil
}
