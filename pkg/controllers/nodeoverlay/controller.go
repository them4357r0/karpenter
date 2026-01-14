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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/reasonable"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/apis/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// Controller for validating NodeOverlay configuration and surfacing conflicts to the user
type Controller struct {
	kubeClient        client.Client
	cloudProvider     cloudprovider.CloudProvider
	clusterState      *state.Cluster
	instanceTypeStore *InstanceTypeStore
	recorder          record.EventRecorder
}

func (c *Controller) Name() string {
	return "nodeoverlay.controller"
}

// NewController constructs a controller for node overlay validation
func NewController(kubeClient client.Client, cp cloudprovider.CloudProvider, instanceTypeStore *InstanceTypeStore, clusterState *state.Cluster, recorder record.EventRecorder) *Controller {
	return &Controller{
		kubeClient:        kubeClient,
		cloudProvider:     cp,
		instanceTypeStore: instanceTypeStore,
		clusterState:      clusterState,
		recorder:          recorder,
	}
}

// Reconcile validates that all node overlays don't have conflicting requirements
func (c *Controller) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	overlayList := &v1alpha1.NodeOverlayList{}
	if err := c.kubeClient.List(ctx, overlayList); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing nodeoverlays, %w", err)
	}
	// Filter out overlays that are being deleted
	overlayList.Items = lo.Filter(overlayList.Items, func(o v1alpha1.NodeOverlay, _ int) bool {
		return o.DeletionTimestamp.IsZero()
	})
	overlayList.OrderByWeight()

	nodePoolList := &v1.NodePoolList{}
	if err := c.kubeClient.List(ctx, nodePoolList); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing nodepools, %w", err)
	}

	statsMap := make(map[string]struct {
		AffectedNodePools         []string
		ImpactedInstanceTypeCount int32
		RunningInstanceCount      int32
	})
	overlayWithRuntimeValidationFailure := map[string]error{}
	overlaysWithConflict := []string{}
	temporaryStore := newInternalInstanceTypeStore()
	nodePoolToInstanceTypes := map[string][]*cloudprovider.InstanceType{}

	// Pre-fetch instance types
	for i := range nodePoolList.Items {
		its, err := c.cloudProvider.GetInstanceTypes(ctx, &nodePoolList.Items[i])
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("listing instance types, %w", err)
		}
		nodePoolToInstanceTypes[nodePoolList.Items[i].Name] = its
	}

	for i := range overlayList.Items {
		overlay := &overlayList.Items[i]
		if err := overlay.RuntimeValidate(ctx); err != nil {
			overlayWithRuntimeValidationFailure[overlay.Name] = err
			continue
		}

		matches, ok := c.validateAndUpdateInstanceTypeOverrides(temporaryStore, nodePoolList.Items, nodePoolToInstanceTypes, *overlay)
		if !ok {
			overlaysWithConflict = append(overlaysWithConflict, overlay.Name)
			continue
		}

		// Calculate stats and record events
		stats := c.calculateStats(matches, c.clusterState)
		statsMap[overlay.Name] = stats

		c.recordEvents(overlay, stats, matches, nodePoolList.Items)
		c.updateMetrics(overlay, stats)
	}

	temporaryStore.evaluatedNodePools.Insert(lo.Map(nodePoolList.Items, func(np v1.NodePool, _ int) string {
		return np.Name
	})...)

	err, requeue := c.updateOverlayStatuses(ctx, overlayList.Items, statsMap, overlaysWithConflict, overlayWithRuntimeValidationFailure)
	if requeue {
		return reconcile.Result{Requeue: true}, nil
	}
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("updating nodeoverlay statuses, %w", err)
	}

	c.instanceTypeStore.UpdateStore(temporaryStore)
	c.clusterState.MarkUnconsolidated()

	return reconcile.Result{RequeueAfter: 6 * time.Hour}, nil
}

func (c *Controller) calculateStats(matches []nodePoolMatch, clusterState *state.Cluster) struct {
	AffectedNodePools         []string
	ImpactedInstanceTypeCount int32
	RunningInstanceCount      int32
} {
	affectedNodePools := lo.FilterMap(matches, func(m nodePoolMatch, _ int) (string, bool) {
		return m.Name, m.MatchedInstanceTypes > 0
	})
	totalInstanceTypes := lo.SumBy(matches, func(m nodePoolMatch) int { return m.MatchedInstanceTypes })
	runningInstanceCount := 0

	// Create a fast lookup map: NodePoolName -> Set of Matched InstanceTypes
	matchMap := make(map[string]map[string]struct{})
	for _, m := range matches {
		if m.MatchedInstanceTypes > 0 {
			if _, exists := matchMap[m.Name]; !exists {
				matchMap[m.Name] = make(map[string]struct{})
			}
			for it := range m.MatchedInstanceTypeNames {
				matchMap[m.Name][it] = struct{}{}
			}
		}
	}

	if len(matchMap) > 0 {
		for node := range clusterState.Nodes() {
			if node.MarkedForDeletion() || !node.Initialized() {
				continue
			}
			nodePoolName := node.Labels()[v1.NodePoolLabelKey]
			instanceType := node.Labels()[corev1.LabelInstanceTypeStable]

			if matchedTypes, ok := matchMap[nodePoolName]; ok {
				if _, matched := matchedTypes[instanceType]; matched {
					runningInstanceCount++
				}
			}
		}
	}
	return struct {
		AffectedNodePools         []string
		ImpactedInstanceTypeCount int32
		RunningInstanceCount      int32
	}{
		AffectedNodePools:         affectedNodePools,
		ImpactedInstanceTypeCount: int32(totalInstanceTypes),   //nolint:gosec
		RunningInstanceCount:      int32(runningInstanceCount), //nolint:gosec
	}
}

func (c *Controller) updateMetrics(overlay *v1alpha1.NodeOverlay, stats struct {
	AffectedNodePools         []string
	ImpactedInstanceTypeCount int32
	RunningInstanceCount      int32
}) {
	NodeOverlayRunningInstances.Set(float64(stats.RunningInstanceCount), map[string]string{"name": overlay.Name})
	NodeOverlayImpactedInstanceTypes.Set(float64(stats.ImpactedInstanceTypeCount), map[string]string{"name": overlay.Name})
}

func (c *Controller) recordEvents(overlay *v1alpha1.NodeOverlay, stats struct {
	AffectedNodePools         []string
	ImpactedInstanceTypeCount int32
	RunningInstanceCount      int32
}, matches []nodePoolMatch, nodePools []v1.NodePool) {
	// Enhanced progressive disclosure message
	matchedTypeSnippet := c.getMatchedTypeSnippet(stats.RunningInstanceCount, matches)

	c.recorder.Eventf(overlay, corev1.EventTypeNormal, "Applied", "Applied to NodePools: %v. Modifying instance types%s", stats.AffectedNodePools, matchedTypeSnippet)

	for _, match := range matches {
		if match.MatchedInstanceTypes > 0 {
			if np, found := lo.Find(nodePools, func(n v1.NodePool) bool { return n.Name == match.Name }); found {
				c.recorder.Eventf(&np, corev1.EventTypeNormal, "NodeOverlayApplied", "NodeOverlay '%s' modified %d instance types.", overlay.Name, match.MatchedInstanceTypes)
			}
		}
	}
}

func (c *Controller) getMatchedTypeSnippet(runningInstanceCount int32, matches []nodePoolMatch) string {
	if runningInstanceCount == 0 {
		return ""
	}

	topN := 3
	var sampleTypes []string
	count := 0
	seenTypes := make(map[string]struct{})

	for _, m := range matches {
		for it := range m.MatchedInstanceTypeNames {
			if _, ok := seenTypes[it]; !ok {
				seenTypes[it] = struct{}{}
				if count < topN {
					sampleTypes = append(sampleTypes, it)
					count++
				}
			}
		}
	}

	snippet := strings.Join(sampleTypes, ", ")
	if len(seenTypes) > topN {
		snippet += fmt.Sprintf(" and %d others", len(seenTypes)-topN)
	}
	return fmt.Sprintf(": %s", snippet)
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	b := controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		// The reconciled overlay does not matter in this case as one reconcile loop
		// will compare every overlay against every other overlay in the cluster.
		For(&v1alpha1.NodeOverlay{}).
		Watches(&v1.NodePool{}, NodeOverlayEventHandler(c.kubeClient)).
		Watches(&corev1.Node{}, NodeOverlayEventHandler(c.kubeClient), builder.WithPredicates(NodeLabelChangePredicate{})).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
			RateLimiter:             reasonable.RateLimiter(),
		})

	for _, nodeClass := range c.cloudProvider.GetSupportedNodeClasses() {
		b.Watches(nodeClass, NodeOverlayEventHandler(c.kubeClient))
	}

	return b.Complete(c)
}

func (c *Controller) validateAndUpdateInstanceTypeOverrides(temporaryStore *internalInstanceTypeStore, nodePoolList []v1.NodePool, nodePoolToInstanceTypes map[string][]*cloudprovider.InstanceType, overlay v1alpha1.NodeOverlay) ([]nodePoolMatch, bool) {
	// Due to reserved capacity type offering being dynamically injected as part of the GetInstanceTypes call
	// We will need to make sure we are validating against each nodepool to make sure. This will ensure that
	// overlays that are targeting reserved instance offerings will be able to apply the offering.
	var matches []nodePoolMatch

	for i := range nodePoolList {
		matchedNames, ok := c.validateInstanceTypesOverride(temporaryStore, nodePoolList[i], nodePoolToInstanceTypes[nodePoolList[i].Name], overlay)
		if !ok {
			return nil, false
		}
		if len(matchedNames) > 0 {
			matches = append(matches, nodePoolMatch{
				Name:                     nodePoolList[i].Name,
				MatchedInstanceTypes:     len(matchedNames),
				MatchedInstanceTypeNames: matchedNames,
			})
		}
	}

	// We separate the validation and storage steps to prevent partial application of invalid node overlays.
	// This two-step process verifies that all instance types across all NodePools are valid before
	// applying any updates, ensuring atomicity of the operation.
	for i := range nodePoolList {
		c.storeUpdatesForInstanceTypeOverride(temporaryStore, nodePoolList[i], nodePoolToInstanceTypes[nodePoolList[i].Name], overlay)
	}

	return matches, true
}

func (c *Controller) validateInstanceTypesOverride(store *internalInstanceTypeStore, nodePool v1.NodePool, its []*cloudprovider.InstanceType, overlay v1alpha1.NodeOverlay) (map[string]struct{}, bool) {
	overlayRequirements := scheduling.NewNodeSelectorRequirements(lo.Map(overlay.Spec.Requirements, func(r v1alpha1.NodeSelectorRequirement, _ int) corev1.NodeSelectorRequirement {
		return r.AsNodeSelectorRequirement()
	})...)

	matchedNames := make(map[string]struct{})
	for _, it := range its {
		offerings := getOverlaidOfferings(nodePool, it, overlayRequirements)
		// This will mean that the overlay does not select on the instance all together
		if len(offerings) == 0 {
			continue
		}
		matchedNames[it.Name] = struct{}{}

		conflictingPriceOverlay := c.isPriceUpdatesConflicting(store, nodePool.Name, it.Name, offerings, overlay)
		conflictingCapacityOverlay := c.isCapacityUpdatesConflicting(store, nodePool.Name, it.Name, overlay)
		// When we find an instance type that is matches a set offering, we will track that based on the
		// overlay that is applied
		if conflictingPriceOverlay || conflictingCapacityOverlay {
			return nil, false
		}
	}

	return matchedNames, true
}

func (c *Controller) storeUpdatesForInstanceTypeOverride(store *internalInstanceTypeStore, nodePool v1.NodePool, its []*cloudprovider.InstanceType, overlay v1alpha1.NodeOverlay) {
	overlayRequirements := scheduling.NewNodeSelectorRequirements(lo.Map(overlay.Spec.Requirements, func(r v1alpha1.NodeSelectorRequirement, _ int) corev1.NodeSelectorRequirement {
		return r.AsNodeSelectorRequirement()
	})...)

	for _, it := range its {
		offerings := getOverlaidOfferings(nodePool, it, overlayRequirements)
		// if we are not able to find any offerings for an instance type
		// This will mean that the overlay does not select on the instance all together
		if len(offerings) == 0 {
			continue
		}

		store.updateInstanceTypeOffering(nodePool.Name, it.Name, overlay, offerings)
		store.updateInstanceTypeCapacity(nodePool.Name, it.Name, overlay)
	}
}

// getOverlaidOfferings will validate that an instance type matches a set of node overlay requirements
// if true, the set of Compatible offering. This function effectively assumes, that if the if there are no offering returned then
// the instance type is not Compatible with the overlay requirements. In cases, were an capacity overlay is being intended to be applied
// based on the offerings, this will be an all or nothing operation. If one offering matches to the requirements
// it will be applied at the instance type level or all the offerings.
func getOverlaidOfferings(nodePool v1.NodePool, it *cloudprovider.InstanceType, overlayReq scheduling.Requirements) cloudprovider.Offerings {
	// The additional requirements will be added to the instance type during scheduling simulation
	// Since getting instance types is done on a NodePool level, these requirements were always assumed
	// to be allowed with these instance types.
	instanceTypeRequirements := scheduling.NewRequirements(
		scheduling.NewRequirement(v1.NodePoolLabelKey, corev1.NodeSelectorOpIn, nodePool.Name),
	)
	instanceTypeRequirements.Add(scheduling.NewLabelRequirements(nodePool.Spec.Template.ObjectMeta.Labels).Values()...)
	instanceTypeRequirements.Add(it.Requirements.Values()...)

	if !instanceTypeRequirements.IsCompatible(overlayReq) {
		return nil
	}
	return it.Offerings.Compatible(overlayReq)
}

func (c *Controller) isPriceUpdatesConflicting(store *internalInstanceTypeStore, nodePoolName string, instanceTypeName string, offerings cloudprovider.Offerings, overlay v1alpha1.NodeOverlay) bool {
	if overlay.Spec.Price == nil && overlay.Spec.PriceAdjustment == nil {
		return false
	}

	for _, of := range offerings {
		if foundConflict := store.isOfferingUpdateConflicting(nodePoolName, instanceTypeName, of, overlay); foundConflict {
			return true
		}
	}
	return false
}

func (c *Controller) isCapacityUpdatesConflicting(store *internalInstanceTypeStore, nodePoolName string, instanceTypeName string, overlay v1alpha1.NodeOverlay) bool {
	if overlay.Spec.Capacity == nil {
		return false
	}

	if foundConflict := store.isCapacityUpdateConflicting(nodePoolName, instanceTypeName, overlay); foundConflict {
		return true
	}
	return false
}

func (c *Controller) updateOverlayStatuses(ctx context.Context, overlayList []v1alpha1.NodeOverlay, statsMap map[string]struct {
	AffectedNodePools         []string
	ImpactedInstanceTypeCount int32
	RunningInstanceCount      int32
}, overlaysWithConflict []string, overlayWithRuntimeValidationFailure map[string]error) (error, bool) {
	errs := make([]error, 0, len(overlayList))
	for i := range overlayList {
		stored := overlayList[i].DeepCopy()

		if stats, ok := statsMap[overlayList[i].Name]; ok {
			overlayList[i].Status.AffectedNodePools = stats.AffectedNodePools
			overlayList[i].Status.ImpactedInstanceTypeCount = lo.ToPtr(stats.ImpactedInstanceTypeCount)
			overlayList[i].Status.RunningInstanceCount = lo.ToPtr(stats.RunningInstanceCount)

			// Update Status Condition
			if stats.ImpactedInstanceTypeCount > 0 {
				overlayList[i].StatusConditions().Set(status.Condition{
					Type:    v1alpha1.ConditionTypeApplied,
					Status:  metav1.ConditionTrue,
					Reason:  "OverlayMatched",
					Message: fmt.Sprintf("Overlay applied to %d NodePools, modifying %d instance types (%d running instances).", len(stats.AffectedNodePools), stats.ImpactedInstanceTypeCount, stats.RunningInstanceCount),
				})
			} else {
				overlayList[i].StatusConditions().Set(status.Condition{
					Type:    v1alpha1.ConditionTypeApplied,
					Status:  metav1.ConditionTrue,
					Reason:  "OverlayMatched",
					Message: "Overlay applied to 0 NodePools.",
				})
			}
		}

		overlayList[i].StatusConditions().SetTrue(v1alpha1.ConditionTypeValidationSucceeded)
		if err, ok := overlayWithRuntimeValidationFailure[overlayList[i].Name]; ok {
			overlayList[i].StatusConditions().SetFalse(v1alpha1.ConditionTypeValidationSucceeded, "RuntimeValidation", err.Error())
		} else if lo.Contains(overlaysWithConflict, overlayList[i].Name) {
			overlayList[i].StatusConditions().SetFalse(v1alpha1.ConditionTypeValidationSucceeded, "Conflict", "conflict with another overlay")
		}

		if !equality.Semantic.DeepEqual(stored, overlayList[i]) {
			// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch
			// can cause races due to the fact that it fully replaces the list on a change
			// Here, we are updating the status condition list
			if err := c.kubeClient.Status().Patch(ctx, &overlayList[i], client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
				// Recompute validation in two scenarios:
				// 1. When encountering a conflict error - this may indicate changes were made to a node overlay
				// 2. When an expected overlay is missing from the cluster - this may indicate a previously
				//    identified validation error might have been resolved
				if errors.IsConflict(err) || errors.IsNotFound(err) {
					return nil, true
				}
				errs = append(errs, err)
			}
		}
	}
	return multierr.Combine(errs...), false
}

// NodeOverlayEventHandler is a watcher on any object to trigger a overlay reconciliation to validate the Node Overlays
// and update the instance type store
func NodeOverlayEventHandler(c client.Client) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
		nodeOverlayList := &v1alpha1.NodeOverlayList{}
		err := c.List(ctx, nodeOverlayList)
		if err != nil {
			return nil
		}
		// Always return requests for ALL existing NodeOverlays ensuring global consistency
		reqs := make([]reconcile.Request, len(nodeOverlayList.Items))
		for i, overlay := range nodeOverlayList.Items {
			reqs[i] = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&overlay)}
		}
		return reqs
	})
}

// NodeLabelChangePredicate filters Node events to only satisfy the requirements if
// the NodePool label or InstanceType label changes.
type NodeLabelChangePredicate struct {
	predicate.Funcs
}

func (NodeLabelChangePredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil || e.ObjectNew == nil {
		return false
	}
	oldNode, ok1 := e.ObjectOld.(*corev1.Node)
	newNode, ok2 := e.ObjectNew.(*corev1.Node)
	if !ok1 || !ok2 {
		return false
	}

	// Trigger if NodePool label changes
	if oldNode.Labels[v1.NodePoolLabelKey] != newNode.Labels[v1.NodePoolLabelKey] {
		return true
	}
	// Trigger if InstanceType label changes
	if oldNode.Labels[corev1.LabelInstanceTypeStable] != newNode.Labels[corev1.LabelInstanceTypeStable] {
		return true
	}

	// Trigger if Node Ready condition changes (optional, but good for "active" count if we filter by Ready)
	// For now, we count all running instances, so maybe just deletion/creation is enough,
	// but Update is called for status changes too. We primarily care about labels.
	// If the node enters a "NotReady" state we might still consider it "Running" until deleted,
	// so sticking to Label changes is the most stable and performant approach for "Configuration" overlays.
	// Actually, if a user manually relabels a node, we want to know.
	return false
}

type nodePoolMatch struct {
	Name                     string
	MatchedInstanceTypes     int
	MatchedInstanceTypeNames map[string]struct{}
}
