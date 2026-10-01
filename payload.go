package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

type payload struct {
	body                 []byte
	ids, times, messages []int
	state                uint64
}

func newPayload(c Config, worker int) *payload {
	p := &payload{body: make([]byte, 0, c.Batch*(c.MessageBytes+256)+16), state: uint64(worker+1) * 0x9e3779b97f4a7c15}
	if c.Format == "logflux" {
		p.body = append(p.body, `{"logs":`...)
	}
	p.body = append(p.body, '[')
	for i := 0; i < c.Batch; i++ {
		if i > 0 {
			p.body = append(p.body, ',')
		}
		field := "eventId"
		level := "INFO"
		if c.Format == "legacy" {
			field = "event_id"
			level = "info"
		}
		if i%20 == 0 {
			level = "ERROR"
			if c.Format == "legacy" {
				level = "error"
			}
		}
		p.body = append(p.body, `{"`...)
		p.body = append(p.body, field...)
		p.body = append(p.body, `":"`...)
		p.ids = append(p.ids, len(p.body))
		p.body = append(p.body, "00000000-0000-4000-8000-000000000000"...)
		p.body = append(p.body, `","timestamp":"`...)
		p.times = append(p.times, len(p.body))
		p.body = append(p.body, "2000-01-01T00:00:00.000000000Z"...)
		p.body = append(p.body, `","service":"service-`...)
		p.body = strconv.AppendInt(p.body, int64((worker+i)%c.Services), 10)
		p.body = append(p.body, `","level":"`...)
		p.body = append(p.body, level...)
		p.body = append(p.body, `","message":"`...)
		p.messages = append(p.messages, len(p.body))
		for j := 0; j < c.MessageBytes; j++ {
			p.body = append(p.body, 'a')
		}
		p.body = append(p.body, `"}`...)
	}
	p.body = append(p.body, ']')
	if c.Format == "logflux" {
		p.body = append(p.body, '}')
	}
	return p
}

func (p *payload) update(prefix [8]byte, seq uint64, batch int) {
	var stamp [40]byte
	timestamp := time.Now().UTC().AppendFormat(stamp[:0], "2006-01-02T15:04:05.000000000Z")
	for i, offset := range p.ids {
		var raw [16]byte
		copy(raw[:8], prefix[:])
		n := seq*uint64(batch) + uint64(i)
		for j := 15; j >= 8; j-- {
			raw[j] = byte(n)
			n >>= 8
		}
		raw[6] = (raw[6] & 0x0f) | 0x40
		raw[8] = (raw[8] & 0x3f) | 0x80
		var encoded [32]byte
		hex.Encode(encoded[:], raw[:])
		b := p.body[offset : offset+36]
		copy(b[0:8], encoded[0:8])
		copy(b[9:13], encoded[8:12])
		copy(b[14:18], encoded[12:16])
		copy(b[19:23], encoded[16:20])
		copy(b[24:36], encoded[20:32])
		copy(p.body[p.times[i]:], timestamp)
		start := p.messages[i]
		end := start
		for p.body[end] != '"' {
			end++
		}
		const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
		for j := start; j < end; j++ {
			p.state ^= p.state << 13
			p.state ^= p.state >> 7
			p.state ^= p.state << 17
			p.body[j] = alphabet[p.state&63]
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
