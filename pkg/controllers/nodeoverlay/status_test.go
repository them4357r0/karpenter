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

package nodeoverlay_test

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/apis/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Status Updates", func() {
	It("should update status and emit events when overlay is applied", func() {
		// Setup a specific instance type with a numerical label for Gte matching
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "default-instance-type",
				Offerings: []*cloudprovider.Offering{
					{
						Available: true,
						Requirements: scheduling.NewLabelRequirements(map[string]string{
							v1.CapacityTypeLabelKey:  "spot",
							corev1.LabelTopologyZone: "test-zone-1",
							"test-capacity":          "20",
						}),
						Price: 1.020,
					},
				},
			}),
		}

		// Update NodePool to have the custom label so IsCompatible passes
		// We need to ensure the NodePool used by the controller validation has the label
		if nodePool.Spec.Template.Labels == nil {
			nodePool.Spec.Template.Labels = map[string]string{}
		}
		nodePool.Spec.Template.Labels["test-capacity"] = "20"
		ExpectApplied(ctx, env.Client, nodePool)

		overlay := test.NodeOverlay(v1alpha1.NodeOverlay{
			Spec: v1alpha1.NodeOverlaySpec{
				Requirements: []v1alpha1.NodeSelectorRequirement{
					{
						Key:      "test-capacity",
						Operator: v1.NodeSelectorOpGte,
						Values:   []string{"10"},
					},
				},
				Weight: lo.ToPtr(int32(10)),
				Price:  lo.ToPtr("5.0"),
			},
		})
		ExpectApplied(ctx, env.Client, overlay)

		// Simulate a running node that matches the overlay
		// We need to add a node to the cluster state that:
		// 1. Has the NodePool label matching the dynamic nodePool.Name
		// 2. Has the InstanceType label "default-instance-type" (which matches the overlay)
		// 3. Has the "test-capacity" label "20" (propagated from instance type)
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: "default-instance-type",
					v1.NodeInitializedLabelKey:     "true",
					"test-capacity":                "20",
				},
			},
			ProviderID: "test-provider-id",
		})
		ExpectApplied(ctx, env.Client, node)

		// Manually update the ClusterState with this node since the controller uses it.
		// In a real environment, this happens via the NodeEventHandler/informer.
		// For the test, we might need to rely on the fact that Env.Cluster is shared.
		// However, the controller creates its own internal state representation via list/watch.
		// Let's ensure the node exists in the API server (done above) and trigger reconcile.
		// The controller's clusterState component (injected in suite_test) should pick it up if it's watching.
		// Wait - checking suite_test.go, we are passing `cluster` to NewController.
		Expect(cluster.UpdateNode(ctx, node)).To(Succeed())

		ExpectReconciled(ctx, nodeOverlayController, reconcile.Request{})

		updatedOverlay := ExpectExists(ctx, env.Client, overlay)
		Expect(updatedOverlay.StatusConditions().IsTrue(v1alpha1.ConditionTypeApplied)).To(BeTrue())
		Expect(updatedOverlay.StatusConditions().Get(v1alpha1.ConditionTypeApplied).Message).To(ContainSubstring("modifying 1 instance types (1 running instances)"))

		// Verify Progressive Disclosure fields
		Expect(updatedOverlay.Status.AffectedNodePools).To(ConsistOf(nodePool.Name))
		Expect(*updatedOverlay.Status.ImpactedInstanceTypeCount).To(Equal(int32(1)))
		Expect(*updatedOverlay.Status.RunningInstanceCount).To(Equal(int32(1)))

		// Check events
		foundOverlayEvent := false
		foundNodePoolEvent := false

		// consume all events
	Loop:
		for {
			select {
			case event := <-recorder.Events:
				if strings.Contains(event, "Applied to NodePools") {
					foundOverlayEvent = true
				}
				if strings.Contains(event, fmt.Sprintf("NodeOverlay '%s' modified 1 instance types", overlay.Name)) {
					foundNodePoolEvent = true
				}
			default:
				break Loop
			}
		}

		Expect(foundOverlayEvent).To(BeTrue(), "Expected overlay event to be emitted")
		Expect(foundNodePoolEvent).To(BeTrue(), "Expected nodepool event to be emitted")
	})
})
