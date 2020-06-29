package external_provider

import (
	"fmt"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sync"
	"time"
)

func NewExternalKafkaProvider(updateInterval time.Duration) (provider.ExternalMetricsProvider, Runnable) {
	metricConverter := NewMetricConverter()
	basicLister := NewBasicMetricLister(updateInterval)
	periodicLister, _ := NewPeriodicMetricLister(basicLister, updateInterval)
	seriesRegistry := NewExternalRegistry(periodicLister)
	return &externalKafkaProvider{
		seriesRegistry:  seriesRegistry,
		metricConverter: metricConverter,
	}, periodicLister
}

type externalKafkaProvider struct {
	metricConverter MetricConverter

	seriesRegistry ExternalRegistry
}

func (p *externalKafkaProvider) GetExternalMetric(namespace string, metricSelector labels.Selector, info provider.ExternalMetricInfo) (*external_metrics.ExternalMetricValueList, error) {
	found, err := p.seriesRegistry.QueryForMetric(namespace, info.Metric)

	if err != nil {
		klog.Errorf("unable to generate a query for the metric: %v", err)
		return nil, apierr.NewInternalError(fmt.Errorf("unable to fetch metrics"))
	}

	if !found {
		return nil, provider.NewMetricNotFoundError(schema.GroupResource{Resource: "namespaces"}, info.Metric)
	}
	// Here is where we're making the query, need to be before here xD
	queryResults := p.seriesRegistry.ValueForMetric(info.Metric)
	return p.metricConverter.Convert(info, queryResults)
}

func (p *externalKafkaProvider) ListAllExternalMetrics() []provider.ExternalMetricInfo {
	return p.seriesRegistry.ListAllMetrics()
}

// converter: start

func NewMetricConverter() MetricConverter {
	return &metricConverter{}
}

type MetricConverter interface {
	Convert(info provider.ExternalMetricInfo, queryResult int) (*external_metrics.ExternalMetricValueList, error)
}

type metricConverter struct {
}

func (c *metricConverter) Convert(info provider.ExternalMetricInfo, queryResult int) (*external_metrics.ExternalMetricValueList, error) {
	result := external_metrics.ExternalMetricValueList{
		Items: []external_metrics.ExternalMetricValue{
			{
				MetricName: info.Metric,
				Timestamp: metav1.Time{
					time.Now(),
				},
				Value: *resource.NewMilliQuantity(int64(queryResult*1000.0), resource.DecimalSI),
			},
		},
	}
	return &result, nil
}

// converter: end

// MetricLister: start

func NewBasicMetricLister(lookback time.Duration) MetricLister {
	lister := basicMetricLister{
		lookback:   lookback,
	}
	return &lister
}

type MetricLister interface {
	ListAllMetrics() (MetricUpdateResult, error)
}

type MetricUpdateResult struct {
	value int
}

type basicMetricLister struct {
	lookback   time.Duration
}

func (l *basicMetricLister) ListAllMetrics() (MetricUpdateResult, error) {
	result := MetricUpdateResult{
		value: 0,
	}
	startTime := time.Now().Add(-1 * l.lookback)
	result.value = startTime.Second()
	return result, nil
}

// MetricLister: end

// PeriodicLister: start
func NewPeriodicMetricLister(realLister MetricLister, updateInterval time.Duration) (MetricListerWithNotification, Runnable) {
	lister := periodicMetricLister{
		updateInterval: updateInterval,
		realLister:     realLister,
		callbacks:      make([]MetricUpdateCallback, 0),
	}
	return &lister, &lister
}

type MetricListerWithNotification interface {
	MetricLister
	Runnable
	AddNotificationReceiver(MetricUpdateCallback)
	UpdateNow()
}

type Runnable interface {
	Run()
	RunUntil(stopChan <-chan struct{})
}

type periodicMetricLister struct {
	realLister       MetricLister
	updateInterval   time.Duration
	mostRecentResult MetricUpdateResult
	callbacks        []MetricUpdateCallback
}

type MetricUpdateCallback func(MetricUpdateResult)

func (l *periodicMetricLister) ListAllMetrics() (MetricUpdateResult, error) {
	return l.mostRecentResult, nil
}

func (l *periodicMetricLister) AddNotificationReceiver(callback MetricUpdateCallback) {
	l.callbacks = append(l.callbacks, callback)
}

func (l *periodicMetricLister) UpdateNow() {
	err := l.updateMetrics()
	if err == nil {
		fmt.Println("error in update now")
	}
}

func (l *periodicMetricLister) updateMetrics() error {
	result, err := l.realLister.ListAllMetrics()

	if err != nil {
		return err
	}

	//Cache the result.
	l.mostRecentResult = result
	//Let our listeners know we've got new data ready for them.
	l.notifyListeners()
	return nil
}

func (l *periodicMetricLister) notifyListeners() {
	for _, listener := range l.callbacks {
		if listener != nil {
			listener(l.mostRecentResult)
		}
	}
}

func (l *periodicMetricLister) Run() {
	l.RunUntil(wait.NeverStop)
}

func (l *periodicMetricLister) RunUntil(stopChan <-chan struct{}) {
	go wait.Until(func() {
		if err := l.updateMetrics(); err != nil {
			utilruntime.HandleError(err)
		}
	}, l.updateInterval, stopChan)
}
// PeriodicLister: end

// registry: start
func NewExternalRegistry(lister MetricListerWithNotification) ExternalRegistry {
	var registry = externalRegistry{
		metrics:     make(map[provider.ExternalMetricInfo]bool, 0),
		metricsValues:     make(map[provider.ExternalMetricInfo]int, 0),
	}
	lister.AddNotificationReceiver(registry.filterAndStoreMetrics)
	return &registry
}

type ExternalRegistry interface {
	// ListAllMetrics lists all metrics known to this registry
	ListAllMetrics() []provider.ExternalMetricInfo
	QueryForMetric(namespace string, metricName string) (bool, error)
	ValueForMetric(meticNAme string) int
}

type externalRegistry struct {
	mu sync.RWMutex
	metrics map[provider.ExternalMetricInfo]bool
	metricsValues map[provider.ExternalMetricInfo]int
}

func (r *externalRegistry) ValueForMetric(metricName string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for key := range r.metricsValues {
		if key.Metric == metricName {
			return r.metricsValues[key]
		}
	}
	return 100
}

func (r *externalRegistry) QueryForMetric(namespace string, metricName string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for key := range r.metrics {
		if key.Metric == metricName {
			return true, nil
		}
	}
	return false, nil
}

func (r *externalRegistry) filterAndStoreMetrics(result MetricUpdateResult) {

	apiMetricsCache := make(map[provider.ExternalMetricInfo]bool, 0)
	apiMetricsValueCache := make(map[provider.ExternalMetricInfo]int, 0)
	apiMetricsCache[provider.ExternalMetricInfo{
			Metric: "queue-size",
	}]=true
	apiMetricsValueCache[provider.ExternalMetricInfo{
		Metric: "queue-size",
	}]=result.value
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metrics = apiMetricsCache
	r.metricsValues = apiMetricsValueCache
}

func (r *externalRegistry) ListAllMetrics() []provider.ExternalMetricInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]provider.ExternalMetricInfo, 0, len(r.metrics))
	for k := range r.metrics {
		keys = append(keys, k)
	}
	return keys
}