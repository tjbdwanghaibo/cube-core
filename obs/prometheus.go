package obs

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// PrometheusText renders the in-process metric registry in the Prometheus text
// exposition format. Timer metrics are exported as count/sum/max/last series.
func PrometheusText(metrics []Metric) []byte {
	var buf bytes.Buffer
	for _, m := range metrics {
		name := prometheusName(m.Name)
		labels := prometheusLabels(m.Labels)
		switch m.Kind {
		case KindCounter:
			writePrometheusSample(&buf, prometheusCounterName(name), labels, m.Value)
		case KindGauge:
			writePrometheusSample(&buf, name, labels, m.Value)
		case KindTimer:
			writePrometheusSample(&buf, name+"_count", labels, m.Count)
			writePrometheusSample(&buf, name+"_sum_nanos", labels, m.TotalNanos)
			writePrometheusSample(&buf, name+"_max_nanos", labels, m.MaxNanos)
			writePrometheusSample(&buf, name+"_last_nanos", labels, m.LastNanos)
		}
	}
	return buf.Bytes()
}

func prometheusCounterName(name string) string {
	if name == "" {
		return ""
	}
	if strings.HasSuffix(name, "_total") {
		return name
	}
	return name + "_total"
}

func writePrometheusSample(buf *bytes.Buffer, name, labels string, value int64) {
	if name == "" {
		return
	}
	if labels == "" {
		fmt.Fprintf(buf, "%s %d\n", name, value)
		return
	}
	fmt.Fprintf(buf, "%s{%s} %d\n", name, labels, value)
}

func prometheusName(name string) string {
	if name == "" {
		return ""
	}
	var b strings.Builder
	for i, r := range name {
		ok := r == '_' || r == ':' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9'
		if ok {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func prometheusLabels(labels Labels) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		name := prometheusName(k)
		if name == "" {
			continue
		}
		parts = append(parts, name+"="+strconv.Quote(labels[k]))
	}
	return strings.Join(parts, ",")
}
