// Package statsd provides an extremely simple statsd client that can
// be used to send metrics to a statsd server.
//
// See http://github.com/etsy/statsd for details.
//
// Messages take the form of "<stat_name>:<magnitude>|<unit>
//
// The sampling mentioned in this API is all done in this client.
//
package statsd

import (
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

const statsdPort = 8125
const namespacePrefix = "brubeck.stats_d"
const timerNamespacePrefix = "brubeck.stats_d.timers"
const timeFormat = "%.2f|ms"
const countFormat = "%d|c"

// Client object that users interact with to send stats to Statsd.
type Client struct {
	prefix   string // must be different across apps
	host     string // the statsd server to send to
	disabled bool   // if true will not send stats. Useful for test/stage/dev
	nc       net.Conn
}

// NewClient creates a new graphite client. Not intended to be used
// more than once per application; call only from from your main
// goroutine.
func NewClient(prefix string, host string, disabled bool) *Client {
	client := &Client{
		host:     host,
		disabled: disabled,
		prefix:   prefix,
	}
	if !disabled {
		client.newUDPSocket()
	}
	return client
}

// If we're sending a sampled value to statsd, we need to increase
// the value proportionally to the sample_rate if we end up sending
// the stat. Otherwise we have to keep track of sample_rate for every
// stat we send and convert in our heads. This way the end value will
// be ~= to the value had we not sampled, but the variance is higher.
func (c *Client) sampleCounts(count int64, sampleRate float32) int64 {
	if sampleRate >= 1.0 {
		return count
	}
	// for sample rates that dont divide count evenly,
	// we have to send a mix of the truncated "count / sample_rate" and
	// the ceil of that. The ratio is the splitThreshold.
	splitThreshold := math.Mod(float64(float32(count)/sampleRate), float64(1))
	if rand.Float64() >= splitThreshold {
		return int64(float32(count) / sampleRate)
	}
	return int64(math.Ceil(float64(float32(count) / sampleRate)))
}

func (c *Client) newUDPSocket() {
	hostname := c.host + ":" + strconv.Itoa(statsdPort)
	conn, _ := net.DialTimeout("udp", hostname, 5*time.Second)
	c.nc = conn
}

func (c *Client) formatStat(stat string, format string, value interface{}, timed bool) string {
	var strFormat string
	if timed {
		strFormat = fmt.Sprintf("%s:%s", strings.Join([]string{timerNamespacePrefix, c.prefix, stat}, "."), format)
	} else {
		strFormat = fmt.Sprintf("%s:%s", strings.Join([]string{namespacePrefix, c.prefix, stat}, "."), format)
	}
	return fmt.Sprintf(strFormat, value)
}

func (c *Client) send(stat string, format string, value interface{}, timed bool) {
	if c.disabled {
		return
	}

	fstat := c.formatStat(stat, format, value, timed)
	c.nc.Write([]byte(fstat))
}

// Incr increments a counter metric by one.
func (c *Client) Incr(stat string) {
	c.IncrBatch(stat, 1)
}

// Decr decrements a counter metric by one.
func (c *Client) Decr(stat string) {
	c.DecrBatch(stat, 1)
}

// Incr increments a counter metric by count.
func (c *Client) IncrBatch(stat string, count int64) {
	c.send(stat, countFormat, count, false)
}

// Decr decrements a counter metric by count.
func (c *Client) DecrBatch(stat string, count int64) {
	c.send(stat, countFormat, -count, false)
}

// sampled returns True if the stat should be sent, otherwise False.
func (c *Client) sampled(sampleRate float32) bool {
	return rand.Float32() <= sampleRate
}

// IncrSampled increments a counter with sampling between 0 and 1.
func (c *Client) IncrSampled(stat string, count int64, sampleRate float32) {
	if c.sampled(sampleRate) {
		sampledCount := c.sampleCounts(count, sampleRate)
		c.IncrBatch(stat, sampledCount)
	}
}

// DecrSampled decrements a counter with sampling between 0 and 1.
func (c *Client) DecrSampled(stat string, count int64, sampleRate float32) {
	if c.sampled(sampleRate) {
		sampledCount := c.sampleCounts(count, sampleRate)
		c.DecrBatch(stat, sampledCount)
	}
}

// Time sends millisecond timing to statsd
func (c *Client) Time(stat string, time float32) {
	c.send(stat, timeFormat, time, true)
}

// SampleTime sends sampled millisecond timing to statsd server
func (c *Client) SampleTime(stat string, time float32, sampleRate float32) {
	if c.sampled(sampleRate) {
		c.send(stat, timeFormat, time, true)
	}
}
