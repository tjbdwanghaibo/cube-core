package obs

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Kind string

const (
	KindCounter               Kind = "counter"
	KindGauge                 Kind = "gauge"
	KindTimer                 Kind = "timer"
	defaultMaxSeriesPerMetric      = 2048
)

type Labels map[string]string

type Metric struct {
	Name        string
	Kind        Kind
	Labels      Labels
	Value       int64
	Count       int64
	TotalNanos  int64
	MaxNanos    int64
	LastNanos   int64
	LastUpdated time.Time
}

type counter struct {
	value       atomic.Int64
	lastUpdated atomic.Int64
}

type gauge struct {
	value       atomic.Int64
	lastUpdated atomic.Int64
}

type timer struct {
	count       atomic.Int64
	totalNanos  atomic.Int64
	maxNanos    atomic.Int64
	lastNanos   atomic.Int64
	lastUpdated atomic.Int64
}

type Registry struct {
	mu                 sync.RWMutex
	counters           map[string]*counter
	gauges             map[string]*gauge
	timers             map[string]*timer
	labels             map[string]Labels
	names              map[string]string
	series             map[string]int
	maxSeriesPerMetric int
	droppedSeries      atomic.Int64
}

var defaultRegistry atomic.Pointer[Registry]

func init() {
	defaultRegistry.Store(NewRegistry())
}

func DefaultRegistry() *Registry {
	if reg := defaultRegistry.Load(); reg != nil {
		return reg
	}
	reg := NewRegistry()
	if defaultRegistry.CompareAndSwap(nil, reg) {
		return reg
	}
	return defaultRegistry.Load()
}

func SetDefaultRegistry(reg *Registry) {
	if reg == nil {
		reg = NewRegistry()
	}
	defaultRegistry.Store(reg)
}

func IncCounter(name string, labels Labels, delta int64) {
	DefaultRegistry().IncCounter(name, labels, delta)
}

func SetGauge(name string, labels Labels, value int64) {
	DefaultRegistry().SetGauge(name, labels, value)
}

func AddGauge(name string, labels Labels, delta int64) {
	DefaultRegistry().AddGauge(name, labels, delta)
}

func ObserveDuration(name string, labels Labels, d time.Duration) {
	DefaultRegistry().ObserveDuration(name, labels, d)
}

func Snapshot() []Metric {
	return DefaultRegistry().Snapshot()
}

type RegistryOption func(*Registry)

func WithMaxSeriesPerMetric(limit int) RegistryOption {
	return func(r *Registry) {
		if r != nil && limit > 0 {
			r.maxSeriesPerMetric = limit
		}
	}
}

func NewRegistry(opts ...RegistryOption) *Registry {
	r := &Registry{
		counters:           make(map[string]*counter),
		gauges:             make(map[string]*gauge),
		timers:             make(map[string]*timer),
		labels:             make(map[string]Labels),
		names:              make(map[string]string),
		series:             make(map[string]int),
		maxSeriesPerMetric: defaultMaxSeriesPerMetric,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

func (r *Registry) IncCounter(name string, labels Labels, delta int64) {
	if r == nil || name == "" || delta == 0 {
		return
	}
	c := r.counter(name, labels)
	if c == nil {
		return
	}
	c.value.Add(delta)
	c.lastUpdated.Store(time.Now().UnixNano())
}

func (r *Registry) SetGauge(name string, labels Labels, value int64) {
	if r == nil || name == "" {
		return
	}
	g := r.gauge(name, labels)
	if g == nil {
		return
	}
	g.value.Store(value)
	g.lastUpdated.Store(time.Now().UnixNano())
}

func (r *Registry) AddGauge(name string, labels Labels, delta int64) {
	if r == nil || name == "" || delta == 0 {
		return
	}
	g := r.gauge(name, labels)
	if g == nil {
		return
	}
	g.value.Add(delta)
	g.lastUpdated.Store(time.Now().UnixNano())
}

func (r *Registry) ObserveDuration(name string, labels Labels, d time.Duration) {
	if r == nil || name == "" {
		return
	}
	nanos := d.Nanoseconds()
	if nanos < 0 {
		nanos = 0
	}
	t := r.timer(name, labels)
	if t == nil {
		return
	}
	t.count.Add(1)
	t.totalNanos.Add(nanos)
	t.lastNanos.Store(nanos)
	t.lastUpdated.Store(time.Now().UnixNano())
	for {
		old := t.maxNanos.Load()
		if nanos <= old || t.maxNanos.CompareAndSwap(old, nanos) {
			break
		}
	}
}

func (r *Registry) Snapshot() []Metric {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Metric, 0, len(r.counters)+len(r.gauges)+len(r.timers))
	for key, c := range r.counters {
		out = append(out, Metric{
			Name:        r.names[key],
			Kind:        KindCounter,
			Labels:      cloneLabels(r.labels[key]),
			Value:       c.value.Load(),
			LastUpdated: unixNanoTime(c.lastUpdated.Load()),
		})
	}
	for key, g := range r.gauges {
		out = append(out, Metric{
			Name:        r.names[key],
			Kind:        KindGauge,
			Labels:      cloneLabels(r.labels[key]),
			Value:       g.value.Load(),
			LastUpdated: unixNanoTime(g.lastUpdated.Load()),
		})
	}
	for key, t := range r.timers {
		out = append(out, Metric{
			Name:        r.names[key],
			Kind:        KindTimer,
			Labels:      cloneLabels(r.labels[key]),
			Count:       t.count.Load(),
			TotalNanos:  t.totalNanos.Load(),
			MaxNanos:    t.maxNanos.Load(),
			LastNanos:   t.lastNanos.Load(),
			LastUpdated: unixNanoTime(t.lastUpdated.Load()),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return labelsKey(out[i].Labels) < labelsKey(out[j].Labels)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (r *Registry) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = make(map[string]*counter)
	r.gauges = make(map[string]*gauge)
	r.timers = make(map[string]*timer)
	r.labels = make(map[string]Labels)
	r.names = make(map[string]string)
	r.series = make(map[string]int)
	r.droppedSeries.Store(0)
}

func (r *Registry) DroppedSeries() int64 {
	if r == nil {
		return 0
	}
	return r.droppedSeries.Load()
}

func (r *Registry) counter(name string, labels Labels) *counter {
	key := metricKey(name, labels)
	r.mu.RLock()
	c := r.counters[key]
	r.mu.RUnlock()
	if c != nil {
		return c
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c = r.counters[key]; c != nil {
		return c
	}
	if !r.reserveSeriesLocked(name, key) {
		return nil
	}
	c = &counter{}
	r.counters[key] = c
	r.labels[key] = cloneLabels(labels)
	r.names[key] = name
	return c
}

func (r *Registry) gauge(name string, labels Labels) *gauge {
	key := metricKey(name, labels)
	r.mu.RLock()
	g := r.gauges[key]
	r.mu.RUnlock()
	if g != nil {
		return g
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if g = r.gauges[key]; g != nil {
		return g
	}
	if !r.reserveSeriesLocked(name, key) {
		return nil
	}
	g = &gauge{}
	r.gauges[key] = g
	r.labels[key] = cloneLabels(labels)
	r.names[key] = name
	return g
}

func (r *Registry) timer(name string, labels Labels) *timer {
	key := metricKey(name, labels)
	r.mu.RLock()
	t := r.timers[key]
	r.mu.RUnlock()
	if t != nil {
		return t
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if t = r.timers[key]; t != nil {
		return t
	}
	if !r.reserveSeriesLocked(name, key) {
		return nil
	}
	t = &timer{}
	r.timers[key] = t
	r.labels[key] = cloneLabels(labels)
	r.names[key] = name
	return t
}

func (r *Registry) reserveSeriesLocked(name string, key string) bool {
	if r == nil || r.maxSeriesPerMetric <= 0 {
		return true
	}
	if _, ok := r.names[key]; ok {
		return true
	}
	if r.series[name] >= r.maxSeriesPerMetric {
		r.droppedSeries.Add(1)
		return false
	}
	r.series[name]++
	return true
}

func metricKey(name string, labels Labels) string {
	if len(labels) == 0 {
		return name
	}
	return name + "{" + labelsKey(labels) + "}"
}

func labelsKey(labels Labels) string {
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
		parts = append(parts, k+"="+strconv.Quote(labels[k]))
	}
	return strings.Join(parts, ",")
}

func cloneLabels(src Labels) Labels {
	if len(src) == 0 {
		return nil
	}
	dst := make(Labels, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func unixNanoTime(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v)
}
