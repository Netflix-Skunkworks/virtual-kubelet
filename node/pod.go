// Copyright © 2017 The virtual-kubelet authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package node

import (
	"context"
	"hash/fnv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	pkgerrors "github.com/pkg/errors"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/trace"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	podStatusReasonProviderFailed = "ProviderFailed"
)

func addPodAttributes(ctx context.Context, span trace.Span, pod *corev1.Pod) context.Context {
	return span.WithFields(ctx, log.Fields{
		"uid":       string(pod.GetUID()),
		"namespace": pod.GetNamespace(),
		"name":      pod.GetName(),
		"phase":     string(pod.Status.Phase),
		"reason":    pod.Status.Reason,
	})
}

func (pc *PodController) createOrUpdatePod(ctx context.Context, pod *corev1.Pod, key string) error {

	ctx, span := trace.StartSpan(ctx, "createOrUpdatePod")
	defer span.End()
	addPodAttributes(ctx, span, pod)

	ctx = span.WithFields(ctx, log.Fields{
		"pod":       pod.GetName(),
		"namespace": pod.GetNamespace(),
	})

	// We do this so we don't mutate the pod from the informer cache
	pod = pod.DeepCopy()
	if err := populateEnvironmentVariables(ctx, pod, pc.resourceManager, pc.recorder); err != nil {
		span.SetStatus(err)
		return err
	}

	// We have to use a  different pod that we pass to the provider than the one that gets used in handleProviderError
	// because the provider  may manipulate the pod in a separate goroutine while we were doing work
	podForProvider := pod.DeepCopy()

	// Check if the pod is already known by the provider.
	// NOTE: Some providers return a non-nil error in their GetPod implementation when the pod is not found while some other don't.
	// Hence, we ignore the error and just act upon the pod if it is non-nil (meaning that the provider still knows about the pod).
	if podFromProvider, _ := pc.provider.GetPod(ctx, pod.Namespace, pod.Name); podFromProvider != nil {
		if !podsEqual(podFromProvider, podForProvider) {
			log.G(ctx).Debugf("Pod %s exists, updating pod in provider", podFromProvider.Name)
			if origErr := pc.provider.UpdatePod(ctx, podForProvider); origErr != nil {
				pc.handleProviderError(ctx, span, origErr, pod)
				return origErr
			}
			log.G(ctx).Info("Updated pod in provider")
		}
	} else {
		if origErr := pc.provider.CreatePod(ctx, podForProvider); origErr != nil {
			pc.handleProviderError(ctx, span, origErr, pod)
			return origErr
		}
		log.G(ctx).Info("Created pod in provider")
	}
	return nil
}

// podsEqual checks if two pods are equal according to the fields we know that are allowed
// to be modified after startup time.
func podsEqual(pod1, pod2 *corev1.Pod) bool {
	// Pod Update Only Permits update of:
	// - `spec.containers[*].image`
	// - `spec.initContainers[*].image`
	// - `spec.activeDeadlineSeconds`
	// - `spec.tolerations` (only additions to existing tolerations)
	// - `objectmeta.labels`
	// - `objectmeta.annotations`
	// compare the values of the pods to see if the values actually changed

	return cmp.Equal(pod1.Spec.Containers, pod2.Spec.Containers) &&
		cmp.Equal(pod1.Spec.InitContainers, pod2.Spec.InitContainers) &&
		cmp.Equal(pod1.Spec.ActiveDeadlineSeconds, pod2.Spec.ActiveDeadlineSeconds) &&
		cmp.Equal(pod1.Spec.Tolerations, pod2.Spec.Tolerations) &&
		cmp.Equal(pod1.ObjectMeta.Labels, pod2.Labels) &&
		cmp.Equal(pod1.ObjectMeta.Annotations, pod2.Annotations)

}

// This is basically the kube runtime's hash container functionality.
// VK only operates at the Pod level so this is adapted for that
func hashPodSpec(spec corev1.PodSpec) uint64 {
	hash := fnv.New32a()
	printer := spew.ConfigState{
		Indent:         " ",
		SortKeys:       true,
		DisableMethods: true,
		SpewKeys:       true,
	}
	printer.Fprintf(hash, "%#v", spec)
	return uint64(hash.Sum32())
}

func (pc *PodController) handleProviderError(ctx context.Context, span trace.Span, origErr error, pod *corev1.Pod) {
	podPhase := corev1.PodPending
	if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
		podPhase = corev1.PodFailed
	}

	pod.ResourceVersion = "" // Blank out resource version to prevent object has been modified error
	pod.Status.Phase = podPhase
	pod.Status.Reason = podStatusReasonProviderFailed
	pod.Status.Message = origErr.Error()

	logger := log.G(ctx).WithFields(log.Fields{
		"podPhase": podPhase,
		"reason":   pod.Status.Reason,
	})

	_, err := pc.client.Pods(pod.Namespace).UpdateStatus(pod)
	if err != nil {
		logger.WithError(err).Warn("Failed to update pod status")
	} else {
		logger.Info("Updated k8s pod status")
	}
	span.SetStatus(origErr)
}

func (pc *PodController) deletePod(ctx context.Context, namespace, name, key string, willRetry bool) (retErr error) {
	ctx, span := trace.StartSpan(ctx, "deletePod")
	defer span.End()


	pod, err := pc.provider.GetPod(ctx, namespace, name)
	// NOTE: Some providers return a non-nil error in their GetPod implementation when the pod is not found while some other don't.
	if errdefs.IsNotFound(err) || pod == nil {
		lastObj, ok := pc.lastPodReceivedFromProvider.Load(key)
		if ok {
			lastPodReceivedFromProvider := lastObj.(*corev1.Pod)
			if lastPodReceivedFromProvider.Status.Phase == corev1.PodSucceeded || lastPodReceivedFromProvider.Status.Phase == corev1.PodFailed {
				return nil
			}
		}
	}
	if err != nil {
		return err
	}

	ctx = addPodAttributes(ctx, span, pod)

	var delErr error
	if delErr = pc.provider.DeletePod(ctx, pod.DeepCopy()); errdefs.IsNotFound(delErr) {
		log.G(ctx).Debug("Pod not found in provider")
		newPod := pod.DeepCopy()
		// Set the pod to failed, this makes sure if the underlying container implementation is gone that a new pod will be created.
		newPod.Status.Phase = corev1.PodFailed
		newPod.Status.Reason = "NotFound"
		pod.Status.Message = "The pod status was not found and may have been deleted from the provider"
		for i, c := range newPod.Status.ContainerStatuses {
			newPod.Status.ContainerStatuses[i].State.Terminated = &corev1.ContainerStateTerminated{
				ExitCode:    -137,
				Reason:      "NotFound",
				Message:     "Container was not found and was likely deleted",
				FinishedAt:  metav1.NewTime(time.Now()),
				StartedAt:   c.State.Running.StartedAt,
				ContainerID: c.ContainerID,
			}
			newPod.Status.ContainerStatuses[i].State.Running = nil
		}

		pc.client.Pods(pod.Namespace).UpdateStatus(newPod)

		return nil
	} else if delErr != nil {
		span.SetStatus(delErr)
		return delErr
	}

	log.G(ctx).Debug("Deleted pod from provider")

	return nil
}

func (pc *PodController) forceDeletePodResource(ctx context.Context, namespace, name string) error {
	ctx, span := trace.StartSpan(ctx, "forceDeletePodResource")
	defer span.End()
	ctx = span.WithFields(ctx, log.Fields{
		"namespace": namespace,
		"name":      name,
	})

	var grace int64
	if err := pc.client.Pods(namespace).Delete(name, &metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		if errors.IsNotFound(err) {
			log.G(ctx).Debug("Pod does not exist in Kubernetes, nothing to delete")
			return nil
		}
		span.SetStatus(err)
		return pkgerrors.Wrap(err, "Failed to delete Kubernetes pod")
	}
	return nil
}

// updatePodStatuses syncs the providers pod status with the kubernetes pod status.
func (pc *PodController) updatePodStatuses(ctx context.Context) {
	ctx, span := trace.StartSpan(ctx, "updatePodStatuses")
	defer span.End()

	// Update all the pods with the provider status.
	pods, err := pc.podsLister.List(labels.Everything())
	if err != nil {
		err = pkgerrors.Wrap(err, "error getting pod list")
		span.SetStatus(err)
		log.G(ctx).WithError(err).Error("Error updating pod statuses")
		return
	}
	ctx = span.WithField(ctx, "nPods", int64(len(pods)))

	for _, pod := range pods {

		status, err := pc.provider.GetPodStatus(ctx, pod.Namespace, pod.Name)
		if errors.IsNotFound(err) {
			continue
		} else if  err != nil {
			log.G(ctx).WithFields(map[string]interface{}{
				"namespace": pod.Namespace,
				"name": pod.Name,
			}).WithError(err).Error("Could not fetch pod status")
		}
		pod = pod.DeepCopy()
		pod.Status = *status
		log.G(ctx).WithField("status", status).Debug()
		pc.enqueuePodStatusUpdate(ctx, pc.podStatusQueue, pod)
	}
}

// updatePodStatus is called with the last pod in the k8s API server.
func (pc *PodController) updatePodStatus(ctx context.Context, pod *corev1.Pod, key string) error {
	ctx, span := trace.StartSpan(ctx, "updatePodStatus")
	defer span.End()
	ctx = addPodAttributes(ctx, span, pod)

	lastObj, ok := pc.lastPodReceivedFromProvider.Load(key)
	// This really should not happen
	if !ok {
		panic("Pod Controller state inconsistent, pod status not found during update processing")
	}
	lastPod := lastObj.(*corev1.Pod)

	// Do not modify the pod that we got from the cache
	pod = pod.DeepCopy()

	// update the pod we got from the cache with the pod status of the pod we got from the provider
	oldStatus := pod.Status
	pod.Status = lastPod.Status

	if _, err := pc.client.Pods(pod.Namespace).UpdateStatus(pod); err != nil {
		span.SetStatus(err)
		return pkgerrors.Wrap(err, "error while updating pod status in kubernetes")
	}

	log.G(ctx).WithFields(log.Fields{
		"new phase":  string(pod.Status.Phase),
		"new reason": pod.Status.Reason,
		"old phase":  string(oldStatus.Phase),
		"old reason": oldStatus.Reason,
	}).Debug("Updated pod status in kubernetes")

	return nil
}

func (pc *PodController) enqueuePodStatusUpdate(ctx context.Context, q workqueue.RateLimitingInterface, pod *corev1.Pod) {
	if key, err := cache.MetaNamespaceKeyFunc(pod); err != nil {
		log.G(ctx).WithError(err).WithField("method", "enqueuePodStatusUpdate").Error("Error getting pod meta namespace key")
	} else {
		// This is a duplicate status suppression mechanism.
		if lastObj, ok := pc.lastPodReceivedFromProvider.Load(key); ok {
			lastPod := lastObj.(*corev1.Pod)
			if cmp.Equal(lastPod.Status, pod.Status) {
				log.G(ctx).Debug("Supressing duplicate pod status update")
				return
			}
		}
		_, err = pc.podsInformer.Lister().Pods(pod.Namespace).Get(pod.Name)
		if errors.IsNotFound(err) {
			log.G(ctx).Debug("Not enqueueing pod status update because pod deleted from api server")
		}

		log.G(ctx).WithField("key", key).Debug("Enqueueing pod status update")
		pc.lastPodReceivedFromProvider.Store(key, pod.DeepCopy())
		q.AddRateLimited(key)
	}
}

func (pc *PodController) podStatusHandler(ctx context.Context, key string, willRetry bool) (retErr error) {
	ctx, span := trace.StartSpan(ctx, "podStatusHandler")
	defer span.End()

	ctx = span.WithField(ctx, "key", key)
	log.G(ctx).Debug("processing pod status update from provider")
	defer func() {
		span.SetStatus(retErr)
		if retErr != nil {
			log.G(ctx).WithError(retErr).Error("Error processing pod status update")
		}
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// Something has gone wrong, so we leak the pod status. We cannot safely delete the pod  status from our internal map,
		// because it might be in a non-terminal state, and another (valid) update may be on the way
		return pkgerrors.Wrap(err, "error splitting cache key")
	}

	pod, err := pc.podsLister.Pods(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			// We received a pod status update _after_ the pod was deleted in API server, so  we skip processing
			pc.lastPodReceivedFromProvider.Delete(key)
			log.G(ctx).WithError(err).Debug("Skipping pod status update for pod missing in Kubernetes")
			return nil
		}
		return pkgerrors.Wrap(err, "error looking up pod")
	}

	return pc.updatePodStatus(ctx, pod, key)
}
