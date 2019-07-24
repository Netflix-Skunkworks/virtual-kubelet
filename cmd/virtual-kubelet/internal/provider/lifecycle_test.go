package provider

// Right now this test lives here because it needs the mock provider, which is an internal module.
// If we move it out of this part of the tree, we can also moves this test.

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/fields"

	"github.com/sirupsen/logrus"

	"github.com/pkg/errors"
	"github.com/virtual-kubelet/virtual-kubelet/cmd/virtual-kubelet/internal/provider/mock"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	logruslogger "github.com/virtual-kubelet/virtual-kubelet/log/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"gotest.tools/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"

	watchutils "k8s.io/client-go/tools/watch"
)

const (
	// There might be a constant we can already leverage here
	testNamespace        = "default"
	informerResyncPeriod = time.Duration(1 * time.Second)
	testNodeName         = "testnode"
	podSyncWorkers       = 3
)

func init() {
	klog.InitFlags(nil)
}

func TestBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newLogger := logruslogger.FromLogrus(logrus.NewEntry(logrus.StandardLogger()))
	logrus.SetLevel(logrus.DebugLevel)
	// Right now, new loggers that are created from spans are broken since log.L isn't set.
	ctx = log.WithLogger(ctx, newLogger)

	// Create the fake client.
	client := fake.NewSimpleClientset()

	podInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(
		client,
		informerResyncPeriod,
		kubeinformers.WithNamespace(testNamespace),
	)
	podInformer := podInformerFactory.Core().V1().Pods()

	// Create another shared informer factory for Kubernetes secrets and configmaps (not subject to any selectors).
	scmInformerFactory := kubeinformers.NewSharedInformerFactory(client, informerResyncPeriod)

	eb := record.NewBroadcaster()
	eb.StartLogging(log.G(ctx).Infof)
	eb.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: client.CoreV1().Events(testNamespace)})
	fakeRecorder := record.NewFakeRecorder(1024)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-fakeRecorder.Events:
				t.Logf("Received event: %s", ev)
			}
		}
	}()

	secretInformer := scmInformerFactory.Core().V1().Secrets()
	configMapInformer := scmInformerFactory.Core().V1().ConfigMaps()
	serviceInformer := scmInformerFactory.Core().V1().Services()

	mockProvider, err := mock.NewMockProviderMockConfig(mock.MockConfig{}, testNodeName, "linux", "1.2.3.4", 0)
	assert.NilError(t, err)
	config := node.PodControllerConfig{
		PodClient:         client.CoreV1(),
		PodInformer:       podInformer,
		EventRecorder:     fakeRecorder,
		Provider:          mockProvider,
		ConfigMapInformer: configMapInformer,
		SecretInformer:    secretInformer,
		ServiceInformer:   serviceInformer,
	}

	pc, err := node.NewPodController(config)
	assert.NilError(t, err)

	p := corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "my-pod",
			Namespace:       testNamespace,
			UID:             "4f20ff31-7775-11e9-893d-000c29a24b34",
			ResourceVersion: "100",
		},
		Spec: corev1.PodSpec{
			NodeName: testNodeName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	listOptions := metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", p.ObjectMeta.Name).String(),
	}

	watchErrCh := make(chan error)

	watcher, err := client.CoreV1().Pods(testNamespace).Watch(listOptions)
	assert.NilError(t, err)
	go func() {
		_, watchErr := watchutils.UntilWithoutRetry(ctx, watcher,
			// Wait for the pod to be created
			// TODO(Sargun): Make this "smarter" about the status the pod is in.
			func(ev watch.Event) (bool, error) {
				pod := ev.Object.(*corev1.Pod)
				return pod.Name == p.ObjectMeta.Name, nil
			})

		watchErrCh <- watchErr
	}()

	go podInformerFactory.Start(ctx.Done())
	go scmInformerFactory.Start(ctx.Done())

	_, e := client.CoreV1().Pods(testNamespace).Create(&p)
	assert.NilError(t, e)

	assert.NilError(t, <-watchErrCh)

	podControllerErrCh := make(chan error, 1)
	go func() {
		podControllerErrCh <- pc.Run(ctx, podSyncWorkers)
	}()

	watcher, err = client.CoreV1().Pods(testNamespace).Watch(listOptions)
	assert.NilError(t, err)
	go func() {

		_, watchErr := watchutils.UntilWithoutRetry(ctx, watcher,
			// Wait for the pod to be started
			func(ev watch.Event) (bool, error) {
				pod := ev.Object.(*corev1.Pod)
				return pod.Status.Phase == corev1.PodRunning, nil
			})

		watchErrCh <- watchErr
	}()

	select {
	case <-ctx.Done():
		t.Fatal("Context ended early")
	case err = <-podControllerErrCh:
		assert.NilError(t, err)
	case err = <-watchErrCh:
		assert.NilError(t, err)

	}

	watcher, err = client.CoreV1().Pods(testNamespace).Watch(listOptions)
	assert.NilError(t, err)
	go func() {
		_, watchErr := watchutils.UntilWithoutRetry(ctx, watcher,
			// Wait for the pod to be started
			func(ev watch.Event) (bool, error) {
				// TODO(Sargun): The pod should have transitioned into some status around failed / succeeded
				// prior to being deleted.
				// In addition, we should check if the deletion timestamp gets set
				return ev.Type == watch.Deleted, nil
			})
		watchErrCh <- watchErr
	}()

	assert.NilError(t, client.CoreV1().Pods(testNamespace).Delete(p.Name, nil))

	select {
	case <-ctx.Done():
		t.Fatal("Context ended early")
	case err = <-podControllerErrCh:
		assert.NilError(t, err)
	case err = <-watchErrCh:
		assert.NilError(t, err)

	}

	cancel()
	assert.NilError(t, <-podControllerErrCh)
}

type pseudoEvent struct {
	event watch.Event
	error error
}
type watchHelper struct {
	w           watch.Interface
	watchEvents chan pseudoEvent
}

func (wh *watchHelper) waitLoop(ctx context.Context) {
	defer close(wh.watchEvents)
	for {
		select {
		case <-ctx.Done():
			wh.w.Stop()
			wh.watchEvents <- pseudoEvent{error: ctx.Err()}
			return
		case event := <-wh.w.ResultChan():
			ev := pseudoEvent{event: event}
			// The only time we explicitly shutdown / close is if there is an error
			if event.Type == watch.Error {
				if status, ok := event.Object.(*metav1.Status); ok {
					ev.error = errors.New(status.Message)
				} else {
					ev.error = errors.New("Encountered unknown errors in event stream")
				}
				wh.watchEvents <- ev
				return
			}
			wh.watchEvents <- ev
		}
	}
}
