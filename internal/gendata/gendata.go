// Package gendata generates deterministic synthetic benchmark inputs.
//
// Every generator is seeded, so the same kind, seed and size always give
// the same bytes. The data is synthetic: it is shaped like English text,
// JSON logs or a mix of content types, but it is not real-world data, and
// compression ratios on it say only how the tools compare on these shapes.
package gendata

import (
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
)

// Kinds lists the accepted kind names.
var Kinds = []string{"text", "logs", "mixed", "random"}

// New returns an endless reader of the given kind, seeded with seed. Wrap
// it in io.LimitReader to get a fixed size.
//
//   - text: English-like words (a Zipf-distributed generated vocabulary)
//     in sentences and paragraphs.
//   - logs: JSON log lines with repeating keys and varying values.
//   - mixed: 256 KiB segments in groups of five, one random, three text and
//     one zeros per group in shuffled order (20% / 60% / 20%).
//   - random: uniformly random bytes (incompressible).
func New(kind string, seed uint64) (io.Reader, error) {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	switch kind {
	case "text":
		return &lineReader{next: newText(r).paragraph}, nil
	case "logs":
		return &lineReader{next: newLogs(r).line}, nil
	case "mixed":
		return newMixed(r), nil
	case "random":
		return &randStream{r: r}, nil
	}
	return nil, fmt.Errorf("gendata: unknown kind %q (want text, logs, mixed or random)", kind)
}

// lineReader serves the output of next, one chunk at a time.
type lineReader struct {
	next func(dst []byte) []byte
	buf  []byte
	pos  int
}

func (l *lineReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if l.pos == len(l.buf) {
			l.buf = l.next(l.buf[:0])
			l.pos = 0
		}
		c := copy(p[n:], l.buf[l.pos:])
		n += c
		l.pos += c
	}
	return n, nil
}

// randStream serves random bytes, eight per Uint64, carrying unused bytes
// over so the output does not depend on read sizes.
type randStream struct {
	r    *rand.Rand
	v    uint64
	left int // unused low bytes of v
}

func (s *randStream) Read(p []byte) (int, error) {
	i := 0
	for ; i < len(p) && s.left > 0; i++ {
		p[i] = byte(s.v)
		s.v >>= 8
		s.left--
	}
	for ; i+8 <= len(p); i += 8 {
		v := s.r.Uint64()
		for k := 0; k < 8; k++ {
			p[i+k] = byte(v >> (8 * k))
		}
	}
	if i < len(p) {
		s.v, s.left = s.r.Uint64(), 8
		for ; i < len(p); i++ {
			p[i] = byte(s.v)
			s.v >>= 8
			s.left--
		}
	}
	return len(p), nil
}

// text generates English-like prose from a fixed generated vocabulary.
type text struct {
	r     *rand.Rand
	vocab [][]byte
	zipf  *rand.Zipf
}

var (
	onsets = []string{"", "b", "c", "d", "f", "g", "h", "l", "m", "n", "p", "r", "s", "t", "w", "th", "st", "br", "ch", "pr", "tr", "sh"}
	nuclei = []string{"a", "e", "i", "o", "u", "ea", "ou", "ai", "io", "ee"}
	codas  = []string{"", "", "n", "r", "s", "t", "l", "nd", "st", "ng", "rt", "ck"}
	// The most frequent words are real English function words, as in
	// natural text.
	common = []string{"the", "of", "and", "to", "a", "in", "is", "that", "for", "it", "as", "was", "with", "be", "by", "on", "not", "he", "this", "are", "or", "his", "from", "at", "which", "but", "have", "an", "had", "they", "you", "were", "their", "one", "all", "we", "can", "her", "has", "there", "been", "if", "more", "when", "will", "would", "who", "so", "no"}
)

const vocabSize = 20000

func newText(r *rand.Rand) *text {
	t := &text{r: r}
	for _, w := range common {
		t.vocab = append(t.vocab, []byte(w))
	}
	for len(t.vocab) < vocabSize {
		var w []byte
		for s := 1 + r.IntN(3); s > 0; s-- {
			w = append(w, onsets[r.IntN(len(onsets))]...)
			w = append(w, nuclei[r.IntN(len(nuclei))]...)
			w = append(w, codas[r.IntN(len(codas))]...)
		}
		t.vocab = append(t.vocab, w)
	}
	t.zipf = rand.NewZipf(r, 1.07, 2, vocabSize-1)
	return t
}

// paragraph appends a paragraph of 3 to 8 sentences and a blank line.
func (t *text) paragraph(dst []byte) []byte {
	for s := 3 + t.r.IntN(6); s > 0; s-- {
		dst = t.sentence(dst)
		dst = append(dst, ' ')
	}
	dst[len(dst)-1] = '\n'
	return append(dst, '\n')
}

func (t *text) sentence(dst []byte) []byte {
	n := 4 + t.r.IntN(18)
	for i := 0; i < n; i++ {
		w := t.vocab[t.zipf.Uint64()]
		if i == 0 {
			dst = append(dst, w[0]-'a'+'A')
			dst = append(dst, w[1:]...)
		} else {
			dst = append(dst, w...)
		}
		switch {
		case i == n-1:
			dst = append(dst, ".!?"[t.r.IntN(10)/8]) // mostly '.'
		case t.r.IntN(12) == 0:
			dst = append(dst, ',', ' ')
		default:
			dst = append(dst, ' ')
		}
	}
	return dst
}

// logs generates JSON log lines.
type logs struct {
	r  *rand.Rand
	ms int64 // milliseconds since 2026-01-01T00:00:00Z
}

var (
	levels   = []string{"info", "info", "info", "info", "info", "info", "debug", "debug", "warn", "error"}
	services = []string{"api", "auth", "billing", "search", "worker", "gateway"}
	methods  = []string{"GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"}
	routes   = []string{"/v1/users/", "/v1/orders/", "/v1/items/", "/v1/sessions/", "/v2/search?q=", "/healthz?id="}
	messages = []string{"request completed", "request completed", "request completed", "cache miss", "retrying upstream call", "slow query", "token refreshed", "rate limit exceeded"}
	statuses = []int{200, 200, 200, 200, 200, 201, 204, 304, 400, 401, 404, 429, 500, 503}
)

func newLogs(r *rand.Rand) *logs { return &logs{r: r} }

const hexDigits = "0123456789abcdef"

func (l *logs) line(dst []byte) []byte {
	l.ms += 1 + l.r.Int64N(40)
	r := l.r
	dst = append(dst, `{"ts":"`...)
	dst = appendTime(dst, l.ms)
	dst = append(dst, `","level":"`...)
	dst = append(dst, levels[r.IntN(len(levels))]...)
	dst = append(dst, `","service":"`...)
	dst = append(dst, services[r.IntN(len(services))]...)
	dst = append(dst, `","msg":"`...)
	dst = append(dst, messages[r.IntN(len(messages))]...)
	dst = append(dst, `","method":"`...)
	dst = append(dst, methods[r.IntN(len(methods))]...)
	dst = append(dst, `","path":"`...)
	dst = append(dst, routes[r.IntN(len(routes))]...)
	dst = strconv.AppendInt(dst, int64(r.IntN(100000)), 10)
	dst = append(dst, `","status":`...)
	dst = strconv.AppendInt(dst, int64(statuses[r.IntN(len(statuses))]), 10)
	dst = append(dst, `,"latency_ms":`...)
	dst = strconv.AppendInt(dst, int64(1+r.ExpFloat64()*30), 10)
	dst = append(dst, `,"user_id":`...)
	dst = strconv.AppendInt(dst, int64(r.IntN(50000)), 10)
	dst = append(dst, `,"request_id":"`...)
	v := r.Uint64()
	for k := 0; k < 16; k++ {
		dst = append(dst, hexDigits[v>>(4*k)&0xf])
	}
	return append(dst, "\"}\n"...)
}

// appendTime appends ms (since 2026-01-01) as 2026-MM-DDTHH:MM:SS.mmmZ,
// using 30-day months; the output only needs to look like a timestamp.
func appendTime(dst []byte, ms int64) []byte {
	pad := func(dst []byte, v int64, w int) []byte {
		s := strconv.AppendInt(nil, v, 10)
		for i := len(s); i < w; i++ {
			dst = append(dst, '0')
		}
		return append(dst, s...)
	}
	sec := ms / 1000
	day := sec / 86400
	dst = append(dst, "2026-"...)
	dst = pad(dst, 1+(day/30)%12, 2)
	dst = append(dst, '-')
	dst = pad(dst, 1+day%30, 2)
	dst = append(dst, 'T')
	dst = pad(dst, (sec/3600)%24, 2)
	dst = append(dst, ':')
	dst = pad(dst, (sec/60)%60, 2)
	dst = append(dst, ':')
	dst = pad(dst, sec%60, 2)
	dst = append(dst, '.')
	dst = pad(dst, ms%1000, 3)
	return append(dst, 'Z')
}

// mixed serves 256 KiB segments: in each group of five, one random, three
// text and one zeros, in an order shuffled per group.
type mixed struct {
	r     *rand.Rand // shuffles each group
	rand  io.Reader
	text  io.Reader
	order [5]byte // 'r', 't' or 'z'
	seg   int     // index into order of the current segment
	left  int     // bytes left in the current segment
}

const segmentSize = 256 << 10

// newMixed gives the shuffle, the random bytes and the text their own
// sources, each seeded from r.
func newMixed(r *rand.Rand) *mixed {
	sub := func() *rand.Rand { return rand.New(rand.NewPCG(r.Uint64(), r.Uint64())) }
	return &mixed{
		r:    sub(),
		rand: &randStream{r: sub()},
		text: &lineReader{next: newText(sub()).paragraph},
		seg:  4,
	}
}

func (m *mixed) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if m.left == 0 {
			m.seg++
			if m.seg == 5 {
				m.order = [5]byte{'r', 't', 't', 't', 'z'}
				m.r.Shuffle(5, func(i, j int) { m.order[i], m.order[j] = m.order[j], m.order[i] })
				m.seg = 0
			}
			m.left = segmentSize
		}
		c := min(len(p)-n, m.left)
		q := p[n : n+c]
		switch m.order[m.seg] {
		case 'r':
			m.rand.Read(q)
		case 't':
			m.text.Read(q)
		case 'z':
			clear(q)
		}
		n += c
		m.left -= c
	}
	return n, nil
}
