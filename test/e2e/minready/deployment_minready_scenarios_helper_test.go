/*
Copyright 2026 The Kruise Authors.

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

package minready

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openkruise/rollouts/api/v1beta1"
	partitiondeployment "github.com/openkruise/rollouts/pkg/controller/batchrelease/control/partitionstyle/deployment"
	"github.com/openkruise/rollouts/pkg/util"
)

func waitMinReadyE2EDeploymentReplicas(namespace string, replicas int32) {
	Eventually(func() bool {
		deployment := &apps.Deployment{}
		key := types.NamespacedName{Namespace: namespace, Name: minReadyE2EDeploymentName}
		Expect(k8sClient.Get(context.TODO(), key, deployment)).Should(Succeed())
		return *deployment.Spec.Replicas == replicas &&
			deployment.Status.ObservedGeneration >= deployment.Generation
	}, 5*time.Minute, time.Second).Should(BeTrue())
}

func waitMinReadyE2EOriginalAvailabilityAnnotations(namespace string, minReadySeconds, progressDeadlineSeconds int32) {
	Eventually(func() bool {
		deployment := &apps.Deployment{}
		key := types.NamespacedName{Namespace: namespace, Name: minReadyE2EDeploymentName}
		Expect(k8sClient.Get(context.TODO(), key, deployment)).Should(Succeed())
		return deployment.Annotations[partitiondeployment.AnnotationOriginalMinReadySeconds] == fmt.Sprintf("%d", minReadySeconds) &&
			deployment.Annotations[partitiondeployment.AnnotationOriginalProgressDeadlineSeconds] == fmt.Sprintf("%d", progressDeadlineSeconds)
	}, 5*time.Minute, time.Second).Should(BeTrue())
}

func waitMinReadyE2EDeploymentRestoredWithAvailability(namespace string, minReadySeconds, progressDeadlineSeconds int32) {
	Eventually(func() bool {
		deployment := &apps.Deployment{}
		key := types.NamespacedName{Namespace: namespace, Name: minReadyE2EDeploymentName}
		Expect(k8sClient.Get(context.TODO(), key, deployment)).Should(Succeed())
		if deployment.Spec.ProgressDeadlineSeconds == nil || *deployment.Spec.ProgressDeadlineSeconds != progressDeadlineSeconds {
			return false
		}
		for _, key := range partitiondeployment.AllOriginalAnnotations {
			if deployment.Annotations[key] != "" {
				return false
			}
		}
		return deployment.Spec.MinReadySeconds == minReadySeconds &&
			deployment.Spec.Strategy.Type == apps.RollingUpdateDeploymentStrategyType
	}, 10*time.Minute, time.Second).Should(BeTrue())
}

func deleteMinReadyE2ERollout(namespace, name string) {
	rollout := &v1beta1.Rollout{}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	Expect(k8sClient.Get(context.TODO(), key, rollout)).Should(Succeed())
	Expect(k8sClient.Delete(context.TODO(), rollout)).Should(Succeed())
}

func waitMinReadyE2ERolloutDeleted(namespace, name string) {
	Eventually(func() bool {
		rollout := &v1beta1.Rollout{}
		key := types.NamespacedName{Namespace: namespace, Name: name}
		return k8sClient.Get(context.TODO(), key, rollout) != nil
	}, 5*time.Minute, time.Second).Should(BeTrue())
}

func restoreMinReadyE2EOriginalAnnotation(namespace, key, value string) {
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment := &apps.Deployment{}
		namespacedName := types.NamespacedName{Namespace: namespace, Name: minReadyE2EDeploymentName}
		if err := k8sClient.Get(context.TODO(), namespacedName, deployment); err != nil {
			return err
		}
		if deployment.Annotations == nil {
			deployment.Annotations = map[string]string{}
		}
		deployment.Annotations[key] = value
		return k8sClient.Update(context.TODO(), deployment)
	})).NotTo(HaveOccurred())
}

func waitMinReadyE2EWebhookEndpointReady() {
	Eventually(func() bool {
		endpoints := &corev1.Endpoints{}
		key := types.NamespacedName{Namespace: "kruise-rollout", Name: "kruise-rollout-webhook-service"}
		if err := k8sClient.Get(context.TODO(), key, endpoints); err != nil {
			return false
		}
		for _, subset := range endpoints.Subsets {
			if len(subset.Addresses) > 0 {
				return true
			}
		}
		return false
	}, 5*time.Minute, time.Second).Should(BeTrue())
}

func waitMinReadyE2EBatchMetricCondition(namespace, name, reason string) {
	waitMinReadyE2EBatchCondition(namespace, name, reason)
}

func waitMinReadyE2EEventReason(namespace, reason string) {
	Eventually(func() bool {
		events := &corev1.EventList{}
		Expect(k8sClient.List(context.TODO(), events, client.InNamespace(namespace))).Should(Succeed())
		for _, event := range events.Items {
			if event.Reason == reason {
				return true
			}
		}
		return false
	}, 5*time.Minute, time.Second).Should(BeTrue())
}

func makeMinReadyE2ERolloutWithReplicas(namespace string, values ...string) *v1beta1.Rollout {
	rollout := newMinReadyE2ERollout(namespace)
	steps := make([]v1beta1.CanaryStep, 0, len(values))
	for _, value := range values {
		steps = append(steps, v1beta1.CanaryStep{Replicas: intstrFromStringPtr(value), Pause: v1beta1.RolloutPause{}})
	}
	rollout.Spec.Strategy.Canary.Steps = steps
	return rollout
}

func intstrFromStringPtr(value string) *intstr.IntOrString {
	parsed := intstr.FromString(value)
	return &parsed
}

func expectMinReadyE2EInflatedMaxUnavailable(namespace string, want int32) {
	waitMinReadyE2EInflatedMaxUnavailable(namespace, want, 5*time.Minute)
}

func waitMinReadyE2EInflatedMaxUnavailable(namespace string, want int32, timeout time.Duration) {
	Eventually(func() bool {
		deployment := &apps.Deployment{}
		key := types.NamespacedName{Namespace: namespace, Name: minReadyE2EDeploymentName}
		Expect(k8sClient.Get(context.TODO(), key, deployment)).Should(Succeed())
		got := deployment.Spec.Strategy.RollingUpdate.MaxUnavailable
		return got != nil && got.IntVal == want
	}, timeout, time.Second).Should(BeTrue(), fmt.Sprintf("want maxUnavailable %d", want))
}

func expectMinReadyE2EOriginalAnnotationAbsent(namespace string) {
	deployment := &apps.Deployment{}
	key := types.NamespacedName{Namespace: namespace, Name: minReadyE2EDeploymentName}
	Expect(k8sClient.Get(context.TODO(), key, deployment)).Should(Succeed())
	Expect(deployment.Annotations[partitiondeployment.AnnotationOriginalMinReadySeconds]).Should(Equal(""))
}

func setMinReadyE2EInitialReplicas(deployment *apps.Deployment, replicas int32) {
	deployment.Spec.Replicas = pointer.Int32(replicas)
}

// expectMinReadyE2EDeploymentPodInvariants asserts that the native Kubernetes
// Deployment controller respects the MaxSurge boundary and has rolled out at
// least minUpdated pods with all updated pods reaching Ready. This verifies
// the native controller behavior (not just the rollout controller's spec patch)
// — the Deployment controller must actually create/terminate pods according to
// the MaxSurge/MaxUnavailable boundaries the MinReady controller sets.
func expectMinReadyE2EDeploymentPodInvariants(namespace, releaseName string, desired, maxSurge, minUpdated int32) {
	Eventually(func() bool {
		deployment := &apps.Deployment{}
		key := types.NamespacedName{Namespace: namespace, Name: minReadyE2EDeploymentName}
		Expect(k8sClient.Get(context.TODO(), key, deployment)).Should(Succeed())
		release := &v1beta1.BatchRelease{}
		Expect(k8sClient.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: releaseName}, release)).Should(Succeed())
		if release.Status.UpdateRevision == "" {
			return false
		}
		updatedReady := minReadyE2EUpdatedReadyPods(namespace, deployment, release.Status.UpdateRevision)
		// Surge invariant: total pods never exceed desired + MaxSurge.
		if deployment.Status.Replicas > desired+maxSurge {
			return false
		}
		// Batch target reached: at least minUpdated pods rolled out.
		if deployment.Status.UpdatedReplicas < minUpdated {
			return false
		}
		// MinReady controller only advances MaxUnavailable when pods become
		// Ready, so every updated pod should be Ready at the paused state.
		if updatedReady < deployment.Status.UpdatedReplicas {
			return false
		}
		return true
	}, 5*time.Minute, time.Second).Should(BeTrue(),
		fmt.Sprintf("pod invariants not satisfied: want minUpdated=%d surgeBound=%d", minUpdated, desired+maxSurge))
}

func minReadyE2EUpdatedReadyPods(namespace string, deployment *apps.Deployment, updateRevision string) int32 {
	pods := &corev1.PodList{}
	listOptions := []client.ListOption{client.InNamespace(namespace)}
	if deployment.Spec.Selector != nil {
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		Expect(err).Should(Succeed())
		listOptions = append(listOptions, client.MatchingLabelsSelector{Selector: selector})
	}
	Expect(k8sClient.List(context.TODO(), pods, listOptions...)).Should(Succeed())
	updatedReady := int32(0)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !util.IsPodActive(pod) || !util.IsConsistentWithRevision(pod.Labels, updateRevision) {
			continue
		}
		ready := util.GetPodReadyCondition(pod.Status)
		if ready != nil && ready.Status == corev1.ConditionTrue {
			updatedReady++
		}
	}
	return updatedReady
}

// expectMinReadyE2EDeploymentFinalState asserts the native Deployment controller
// reached the fully rolled-out steady state: all pods updated, ready, no surge.
func expectMinReadyE2EDeploymentFinalState(namespace string, desired int32) {
	Eventually(func() bool {
		deployment := &apps.Deployment{}
		key := types.NamespacedName{Namespace: namespace, Name: minReadyE2EDeploymentName}
		Expect(k8sClient.Get(context.TODO(), key, deployment)).Should(Succeed())
		return deployment.Status.UpdatedReplicas == desired &&
			deployment.Status.ReadyReplicas == desired &&
			deployment.Status.Replicas == desired
	}, 5*time.Minute, time.Second).Should(BeTrue(),
		fmt.Sprintf("final state not reached: want Updated/Ready/Replicas=%d", desired))
}
