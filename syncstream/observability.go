package syncstream

type MetricSink interface {
	Gauge(name string, value float64, labels map[string]string)
}

type HealthOptions struct {
	MaxPending uint64
	MaxDropped uint64
	MaxStreams int
}
type HealthStatus struct {
	Healthy bool
	Reason  string
	Metrics HistoryMetrics
}

func (history *History) Health(options HealthOptions) HealthStatus {
	metrics := history.Metrics()
	status := HealthStatus{Healthy: true, Metrics: metrics}
	if options.MaxPending > 0 && metrics.Pending > options.MaxPending {
		status.Healthy, status.Reason = false, "pending_limit"
	} else if options.MaxDropped > 0 && metrics.Dropped > options.MaxDropped {
		status.Healthy, status.Reason = false, "dropped_limit"
	} else if options.MaxStreams > 0 && metrics.Streams > options.MaxStreams {
		status.Healthy, status.Reason = false, "stream_limit"
	}
	return status
}

func (history *History) ExportMetrics(sink MetricSink, labels map[string]string) {
	if sink == nil {
		return
	}
	metrics := history.Metrics()
	sink.Gauge("syncstream_epoch", float64(metrics.Epoch), labels)
	sink.Gauge("syncstream_streams", float64(metrics.Streams), labels)
	sink.Gauge("syncstream_retained", float64(metrics.Retained), labels)
	sink.Gauge("syncstream_dropped", float64(metrics.Dropped), labels)
	sink.Gauge("syncstream_pending", float64(metrics.Pending), labels)
}
