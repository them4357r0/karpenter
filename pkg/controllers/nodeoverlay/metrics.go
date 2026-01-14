/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nodeoverlay

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	nodeOverlaySubsystem = "nodeoverlay"
)

var (
	NodeOverlayRunningInstances = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: nodeOverlaySubsystem,
			Name:      "running_instances",
			Help:      "Number of running instances matching the node overlay. Labeled by overlay name.",
		},
		[]string{
			"name",
		},
	)
	NodeOverlayImpactedInstanceTypes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: nodeOverlaySubsystem,
			Name:      "impacted_instance_types",
			Help:      "Number of instance types impacted by the node overlay. Labeled by overlay name.",
		},
		[]string{
			"name",
		},
	)
)
