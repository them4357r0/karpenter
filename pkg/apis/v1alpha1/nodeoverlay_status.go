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

package v1alpha1

import (
	"github.com/awslabs/operatorpkg/status"
)

const (
	// ConditionTypeValidationSucceeded = "ValidationSucceeded" condition indicates that the
	// runtime-based configuration is valid and conflict for this NodeOverlay
	ConditionTypeValidationSucceeded = "ValidationSucceeded"
	// ConditionTypeApplied indicates that the overlay has been successfully applied to at least one NodePool/InstanceType.
	ConditionTypeApplied = "Applied"
)

// NodeOverlayStatus defines the observed state of NodeOverlay
type NodeOverlayStatus struct {
	//nolint:kubeapilinter
	// Conditions contains signals for health and readiness
	// +optional
	Conditions []status.Condition `json:"conditions,omitempty"` //nolint:kubeapilinter

	// affectedNodePools lists the names of NodePools to which this overlay is applied.
	// +listType=atomic
	// +optional
	AffectedNodePools []string `json:"affectedNodePools,omitempty"`

	// impactedInstanceTypeCount is the total number of instance types across all NodePools that are modified by this overlay.
	// +optional
	ImpactedInstanceTypeCount *int32 `json:"impactedInstanceTypeCount,omitempty"`

	// runningInstanceCount is the total number of actual running nodes/instances that verify the overlay's criteria.
	// This provides a real-time view of the overlay's impact on the current cluster.
	// +optional
	RunningInstanceCount *int32 `json:"runningInstanceCount,omitempty"`
}

func (in *NodeOverlay) StatusConditions() status.ConditionSet {
	return status.NewReadyConditions(
		ConditionTypeValidationSucceeded,
		ConditionTypeApplied,
	).For(in)
}

func (in *NodeOverlay) GetConditions() []status.Condition {
	return in.Status.Conditions
}

func (in *NodeOverlay) SetConditions(conditions []status.Condition) {
	in.Status.Conditions = conditions
}
