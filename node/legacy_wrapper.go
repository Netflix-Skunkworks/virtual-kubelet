package node

import (
	"context"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/trace"
	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"

	v1 "k8s.io/api/core/v1"
)

var (
	_ PodLifecycleHandlerV1 = (*LegacyPodLifecycleHandlerWrapper)(nil)
)

type podHolder struct {
	deletionTimer     *time.Timer
	scheduledDeletion *time.Time
	pod               *v1.Pod
}

type podKey struct {
	namespace string
	name      string
}

type LegacyPodLifecycleHandlerWrapper struct {
	knownPodsLock sync.Mutex
	knownPods     map[podKey]*podHolder
	reconcileTime time.Duration
	PodLifecycleHandler
}

// WrapLegacyPodLifecycleHandler allows you to use a PodLifecycleHandler which does not implement NotifyPods.
// It runs a background loop, based on reconcileTime. GetPods will be called at this time. No new pods must be created
// or exposed by the provider unless it's through CreatePod.
func WrapLegacyPodLifecycleHandler(ctx context.Context, handler PodLifecycleHandler, reconcileTime time.Duration) (*LegacyPodLifecycleHandlerWrapper, error) {
	providerPods, err := handler.GetPods(ctx)
	if err != nil {
		return nil, err
	}

	wrapper := &LegacyPodLifecycleHandlerWrapper{
		PodLifecycleHandler: handler,
		knownPods:           make(map[podKey]*podHolder),
		reconcileTime:       reconcileTime,
	}
	for _, pod := range providerPods {
		wrapper.knownPods[podKey{namespace: pod.Namespace, name: pod.Name}] = &podHolder{
			pod: pod.DeepCopy(),
		}
	}

	return wrapper, nil
}

// NotifyPods kicks off the reconciliation loop. It will continue until the context is cancelled.
func (wrapper *LegacyPodLifecycleHandlerWrapper) NotifyPods(ctx context.Context, notifier func(*v1.Pod)) {
	// TODO: Consider adding protection from this being called multiple times.
	go wrapper.loop(ctx, notifier)
}

func (wrapper *LegacyPodLifecycleHandlerWrapper) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	podCopy := pod.DeepCopy()
	err := wrapper.PodLifecycleHandler.UpdatePod(ctx, pod)
	if err == nil {
		wrapper.knownPodsLock.Lock()
		defer wrapper.knownPodsLock.Unlock()
		key := podKey{namespace: pod.Namespace, name: pod.Name}
		if _, ok := wrapper.knownPods[key]; ok {
			wrapper.knownPods[key].pod = podCopy
		}
	}
	return err
}

func (wrapper *LegacyPodLifecycleHandlerWrapper) CreatePod(ctx context.Context, pod *v1.Pod) error {
	podCopy := pod.DeepCopy()
	err := wrapper.PodLifecycleHandler.CreatePod(ctx, pod)
	if err == nil {
		wrapper.knownPodsLock.Lock()
		defer wrapper.knownPodsLock.Unlock()
		// CreatePod MUST never be called after DeletePod. Therefore, we can arbitrarily overwrite this field
		wrapper.knownPods[podKey{namespace: pod.Namespace, name: pod.Name}] = &podHolder{
			pod: podCopy,
		}
	}

	return err
}

func (wrapper *LegacyPodLifecycleHandlerWrapper) DeletePod(ctx context.Context, pod *v1.Pod) error {
	err := wrapper.PodLifecycleHandler.DeletePod(ctx, pod)

	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}

	wrapper.knownPodsLock.Lock()
	defer wrapper.knownPodsLock.Unlock()
	// This means that it's okay to delete our tracking of the pod because the error was either the pod isn't found (the
	// provider may have "lost" it), or deletion was successful
	key := podKey{namespace: pod.Namespace, name: pod.Name}
	knownPod, ok := wrapper.knownPods[key]
	// We never knew about this pod. That's fine.
	if !ok {
		return err
	}

	// This pod is scheduled for immediate deletion
	if pod.DeletionGracePeriodSeconds == nil || *pod.DeletionGracePeriodSeconds == 0 {
		delete(wrapper.knownPods, key)
		return err
	}

	gracePeriod := time.Second * time.Duration(*pod.DeletionGracePeriodSeconds)

	// This pod was already scheduled for deletion
	if knownPod.scheduledDeletion != nil {
		// The time to scheduled deletion is shorter than the new gracePeriod. We don't ever *increase* the cleanup
		// time.
		if time.Until(*knownPod.scheduledDeletion) < gracePeriod {
			return err
		}

		// Whoops, we missed the deletionTimer. That's fine.
		if !knownPod.deletionTimer.Stop() {
			return err
		}
	}

	scheduledDeletion := time.Now().Add(gracePeriod)
	knownPod.scheduledDeletion = &scheduledDeletion
	knownPod.deletionTimer = time.AfterFunc(gracePeriod, func() {
		wrapper.knownPodsLock.Lock()
		defer wrapper.knownPodsLock.Unlock()
		delete(wrapper.knownPods, key)
	})

	return err
}

func (wrapper *LegacyPodLifecycleHandlerWrapper) loop(ctx context.Context, notifier func(*v1.Pod)) {
	timer := time.NewTimer(wrapper.reconcileTime)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			wrapper.reconcile(ctx, notifier)
			timer.Reset(wrapper.reconcileTime)
		}
	}
}

func (wrapper *LegacyPodLifecycleHandlerWrapper) reconcile(ctx context.Context, notifier func(*v1.Pod)) {
	ctx, span := trace.StartSpan(ctx, "reconcile")
	defer span.End()
	// Gather the pod keys
	wrapper.knownPodsLock.Lock()
	knownPods := make(map[podKey]*v1.Pod, len(wrapper.knownPods))
	for key, pod := range wrapper.knownPods {
		// This should be fine, since pods are never changed.
		knownPods[key] = pod.pod
	}
	wrapper.knownPodsLock.Unlock()

	// TODO: Think about adding concurrency
	for key, pod := range knownPods {
		l := log.G(ctx).WithFields(map[string]interface{}{"name": key.name, "namespace": key.namespace})
		if podStatus, err := wrapper.GetPodStatus(ctx, key.namespace, key.name); errors.IsNotFound(err) {
			wrapper.knownPodsLock.Lock()
			delete(wrapper.knownPods, key)
			wrapper.knownPodsLock.Unlock()
		} else if err != nil {
			l.WithError(err).Error("Unable to fetch pod status")
		} else if podStatus == nil {
			// TODO: Consider deleting the pod from our pod database here
			l.Error("Pod status is nil")
		} else {
			l.Debug("Notified on pod")
			pod.Status = *podStatus
			notifier(pod)
		}
	}
}
