package main

import (
	"flag"
	basecmd "github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/cmd"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
	"os"
	"test-rest/pkg/external-provider"
	"time"
)

func main()  {
	logs.InitLogs()
	defer logs.FlushLogs()

	cmd := &KafkaAdapter{
		MetricsRelistInterval: 10 * time.Second,
		MetricsMaxAge:         20 * time.Second,
	}
	cmd.Name = "kafka-adapter"
	cmd.addFlags()
	cmd.Flags().AddGoFlagSet(flag.CommandLine)
	if err := cmd.Flags().Parse(os.Args); err != nil {
		klog.Fatalf("unable to parse flags: %v", err)
	}

	// construct the external provider
	emProvider, err := cmd.makeExternalProvider(wait.NeverStop)
	if err != nil {
		klog.Fatalf("unable to construct external metrics provider: %v", err)
	}

	// attach the provider to the server, if it's needed
	if emProvider != nil {
		cmd.WithExternalMetrics(emProvider)
	}

	// run the server
	if err := cmd.Run(wait.NeverStop); err != nil {
		klog.Fatalf("unable to run custom metrics adapter: %v", err)
	}
}



type KafkaAdapter struct {
	basecmd.AdapterBase

	// MetricsRelistInterval is the interval at which to relist the set of available metrics
	MetricsRelistInterval time.Duration
	// MetricsMaxAge is the period to query available metrics for
	MetricsMaxAge time.Duration
}

func (cmd *KafkaAdapter) addFlags() {
	cmd.Flags().DurationVar(&cmd.MetricsRelistInterval, "metrics-relist-interval", cmd.MetricsRelistInterval, ""+
		"interval at which to re-list the set of all available metrics from kafka")
	cmd.Flags().DurationVar(&cmd.MetricsMaxAge, "metrics-max-age", cmd.MetricsMaxAge, ""+
		"period for which to query the set of available metrics from Kafka")
}

func (cmd *KafkaAdapter) makeExternalProvider(stopCh <-chan struct{}) (provider.ExternalMetricsProvider, error) {

	// construct the provider and start it
	emProvider, runner := external_provider.NewExternalKafkaProvider(cmd.MetricsRelistInterval)
	runner.RunUntil(stopCh)

	return emProvider, nil
}