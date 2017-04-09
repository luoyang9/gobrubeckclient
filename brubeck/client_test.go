package statsd

import (
	"math"
	"strings"
	"testing"
)

const testAppname = "appname"

var formatTests = []struct {
	stat   string
	format string
	value  interface{}
	timed  bool
	out    string
}{
	{"hits", countFormat, 1, false, "brubeck.stats_d.appname.hits:1|c"},
	{"qps", timeFormat, 20.004, true, "brubeck.stats_d.timers.appname.qps:20.00|ms"},
}

var sampledCountsTests = []struct {
	count       int64
	sampleRate  float32
	attempts    int64
	errorMargin float64
}{
	{100, 0.5, 1000, 0.01},
	{77, 0.125, 1000, 0.01},
}

func disabledClient() *Client {
	return NewClient(testAppname, "statsd.i.wish.com", true)
}

func TestPrefixT(t *testing.T) {
	client := disabledClient()
	if client.prefix != testAppname {
		t.Error("incorrect prefix, received: " + client.prefix)
	}
}

func TestStatParser(t *testing.T) {
	client := disabledClient()

	for _, tt := range formatTests {
		s := client.formatStat(tt.stat, tt.format, tt.value, tt.timed)
		if s != tt.out {
			t.Errorf("%s != %s", s, tt.out)
		}
	}
}

func TestSampledCounts(t *testing.T) {
	client := disabledClient()
	for _, tt := range sampledCountsTests {
		var sum int64 = 0
		for i := int64(0); i < tt.attempts; i++ {
			sum += client.sampleCounts(tt.count, tt.sampleRate)
		}
		sum = int64(float32(sum) * tt.sampleRate)
		nonsampledSum := tt.count * tt.attempts
		errorMargin := math.Abs(float64(nonsampledSum-sum)) / float64(nonsampledSum)
		if errorMargin >= tt.errorMargin {
			t.Errorf("Margin of error too high: %f", errorMargin)
		}
	}
}

func TestFormatStat(t *testing.T) {
	client := disabledClient()
	formatted := client.formatStat("asdf", timeFormat, float32(2), true)
	if !strings.Contains(formatted, timerNamespacePrefix) {
		t.Errorf("Timer stat does not contain timer namespace prefix")
	}
	formatted = client.formatStat("asdf", countFormat, float32(2), false)
	if !strings.Contains(formatted, namespacePrefix) {
		t.Errorf("Count stat does not contain count namespace prefix")
	}
}
