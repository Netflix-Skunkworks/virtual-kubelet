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

	"github.com/sirupsen/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	loglogrus "github.com/virtual-kubelet/virtual-kubelet/log/logrus"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"

	//"k8s.io/client-go/kubernetes"
	//"k8s.io/client-go/util/retry"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	//"sync"
	"testing"
	"time"

	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
	coord "k8s.io/api/coordination/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	testclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestNodeRun(t *testing.T) {
	t.Run("WithoutLease", func(t *testing.T) { testNodeRun(t, false) })
	t.Run("WithLease", func(t *testing.T) { testNodeRun(t, true) })
}

func TestNodeConditionsPatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lgr := logrus.New()
	lgr.SetLevel(logrus.DebugLevel)
	log.WithLogger(ctx, loglogrus.FromLogrus(lgr.WithFields(nil)))

	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{},
		KubeAPIServerFlags: []string{
			//		"-v=100",
			"--advertise-address=127.0.0.1",
			"--etcd-servers={{ if .EtcdURL }}{{ .EtcdURL.String }}{{ end }}",
			"--cert-dir={{ .CertDir }}",
			"--insecure-port={{ if .URL }}{{ .URL.Port }}{{ end }}",
			"--insecure-bind-address={{ if .URL }}{{ .URL.Hostname }}{{ end }}",
			"--secure-port={{ if .SecurePort }}{{ .SecurePort }}{{ end }}",
			// we're keeping this disabled because if enabled, default SA is missing which would force all tests to create one
			// in normal apiserver operation this SA is created by controller, but that is not run in integration environment
			"--disable-admission-plugins=ServiceAccount",
			"--service-cluster-ip-range=10.0.0.0/24",
			"--allow-privileged=true",
			"--audit-log-path=audit.log",
			"--audit-policy-file=../audit.yaml",
		},
	}

	cfg, err := testEnv.Start()
	assert.NilError(t, err)
	defer testEnv.Stop()
	defer time.Sleep(time.Second * 10)

	assert.NilError(t, clientgoscheme.AddToScheme(scheme.Scheme))

	var k8sClient client.Client
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})

	err = k8sClient.List(ctx, &corev1.PodList{})
	assert.NilError(t, err)

	_ = ctx
	clientset, err := kubernetes.NewForConfig(cfg)
	assert.NilError(t, err)
	nodes := clientset.CoreV1().Nodes()

	baseCondition := corev1.NodeCondition{
		Type:    "BaseCondition",
		Status:  "True",
		Reason:  "NA",
		Message: "This is a condition that should always be there",
	}

	testProvider := &testNodeProvider{
		NodeProvider: &NaiveNodeProvider{},
	}

	testNode := testNode(t)
	// We have to refer to testNodeCopy during the course of the test. testNode is modified by the node controller
	// so it will trigger the race detector.
	testNodeCopy := testNode.DeepCopy()
	node, err := NewNodeController(testProvider, testNode, clientset.CoreV1().Nodes(), WithNodePingInterval(time.Second), WithNodeStatusUpdateInterval(2*time.Second))
	assert.NilError(t, err)

	chErr := make(chan error)
	defer func() {
		cancel()
		assert.NilError(t, <-chErr)
	}()

	go func() {
		chErr <- node.Run(ctx)
		close(chErr)
	}()

	//nw := makeWatch(t, nodes, testNodeCopy.Name)
	nw, err := nodes.Watch(metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("metadata.name", testNodeCopy.Name).String()})
	assert.NilError(t, err)
	defer nw.Stop()
	nr := nw.ResultChan()

	// Wait for the node to exist
	klog.Infoln("Waiting for node")
	for e := range nr {
		klog.Infof("Got initial node: %v", e)
		break
	}

	testProvider.callbackStatus(&corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				baseCondition,
			},
		},
	})

	for e := range nr {
		klog.Infof("Got node: %v", e)
		if len(e.Object.(*corev1.Node).Status.Conditions) > 0 {
			klog.Infof("Saw condition, falling through")
			break
		}
	}

	manuallyAddedCondition := corev1.NodeCondition{
		Type:              "manuallyAddedCondition",
		Status:            "True",
		Reason:            "manuallyAddedConditionAdded",
		Message:           "we added a condition manually",
		LastHeartbeatTime: metav1.Now(),
	}
	assert.NilError(t, retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node, err := nodes.Get(testNodeCopy.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		node.Status.Conditions = append(node.Status.Conditions, manuallyAddedCondition)

		node, err = nodes.UpdateStatus(node)
		klog.Infof("Updated node: %v", node)
		return err
	}))

	newCondition := corev1.NodeCondition{
		Type:    "NewCondition",
		Status:  "True",
		Reason:  "NA",
		Message: "This is a new condition brought on by VK.",
	}

	testProvider.callbackStatus(&corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				baseCondition,
				newCondition,
			},
		},
	})

	// We've added the manual condition, now to ask the status update to add our test condition
	// we can then see if both the test condition and the manually added condition show up in a status update

	for result := range nr {
		node := result.Object.(*corev1.Node)
		klog.Infof("Node: %v", node)
		if len(node.Status.Conditions) == 3 {
			break
		}
	}
}

func testNodeRun(t *testing.T, enableLease bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := testclient.NewSimpleClientset()

	testP := &testNodeProvider{NodeProvider: &NaiveNodeProvider{}}

	nodes := c.CoreV1().Nodes()
	leases := c.CoordinationV1beta1().Leases(corev1.NamespaceNodeLease)

	interval := 1 * time.Millisecond
	opts := []NodeControllerOpt{
		WithNodePingInterval(interval),
		WithNodeStatusUpdateInterval(interval),
	}
	if enableLease {
		opts = append(opts, WithNodeEnableLeaseV1Beta1(leases, nil))
	}
	testNode := testNode(t)
	// We have to refer to testNodeCopy during the course of the test. testNode is modified by the node controller
	// so it will trigger the race detector.
	testNodeCopy := testNode.DeepCopy()
	node, err := NewNodeController(testP, testNode, nodes, opts...)
	assert.NilError(t, err)

	chErr := make(chan error)
	defer func() {
		cancel()
		assert.NilError(t, <-chErr)
	}()

	go func() {
		chErr <- node.Run(ctx)
		close(chErr)
	}()

	nw := makeWatch(t, nodes, testNodeCopy.Name)
	defer nw.Stop()
	nr := nw.ResultChan()

	lw := makeWatch(t, leases, testNodeCopy.Name)
	defer lw.Stop()
	lr := lw.ResultChan()

	var (
		lBefore      *coord.Lease
		nodeUpdates  int
		leaseUpdates int

		iters         = 50
		expectAtLeast = iters / 5
	)

	timeout := time.After(30 * time.Second)
	for i := 0; i < iters; i++ {
		var l *coord.Lease

		select {
		case <-timeout:
			t.Fatal("timed out waiting for expected events")
		case <-time.After(time.Second):
			t.Errorf("timeout waiting for event")
			continue
		case err := <-chErr:
			t.Fatal(err) // if this returns at all it is an error regardless if err is nil
		case <-nr:
			nodeUpdates++
			continue
		case le := <-lr:
			l = le.Object.(*coord.Lease)
			leaseUpdates++

			assert.Assert(t, cmp.Equal(l.Spec.HolderIdentity != nil, true))
			assert.NilError(t, err)
			assert.Check(t, cmp.Equal(*l.Spec.HolderIdentity, testNodeCopy.Name))
			if lBefore != nil {
				assert.Check(t, before(lBefore.Spec.RenewTime.Time, l.Spec.RenewTime.Time))
			}

			lBefore = l
		}
	}

	lw.Stop()
	nw.Stop()

	assert.Check(t, atLeast(nodeUpdates, expectAtLeast))
	if enableLease {
		assert.Check(t, atLeast(leaseUpdates, expectAtLeast))
	} else {
		assert.Check(t, cmp.Equal(leaseUpdates, 0))
	}

	// trigger an async node status update
	n, err := nodes.Get(testNode.Name, metav1.GetOptions{})
	assert.NilError(t, err)
	newCondition := corev1.NodeCondition{
		Type:               corev1.NodeConditionType("UPDATED"),
		LastTransitionTime: metav1.Now().Rfc3339Copy(),
	}
	n.Status.Conditions = append(n.Status.Conditions, newCondition)

	nw = makeWatch(t, nodes, testNodeCopy.Name)
	defer nw.Stop()
	nr = nw.ResultChan()

	testP.triggerStatusUpdate(n)

	eCtx, eCancel := context.WithTimeout(ctx, 10*time.Second)
	defer eCancel()

	select {
	case err := <-chErr:
		t.Fatal(err) // if this returns at all it is an error regardless if err is nil
	case err := <-waitForEvent(eCtx, nr, func(e watch.Event) bool {
		node := e.Object.(*corev1.Node)
		if len(node.Status.Conditions) == 0 {
			return false
		}

		// Check if this is a node update we are looking for
		// Since node updates happen periodically there could be some that occur
		// before the status update that we are looking for happens.
		c := node.Status.Conditions[len(n.Status.Conditions)-1]
		if !c.LastTransitionTime.Equal(&newCondition.LastTransitionTime) {
			return false
		}
		if c.Type != newCondition.Type {
			return false
		}
		return true
	}):
		assert.NilError(t, err, "error waiting for updated node condition")
	}
}

func TestNodeCustomUpdateStatusErrorHandler(t *testing.T) {
	c := testclient.NewSimpleClientset()
	testP := &testNodeProvider{NodeProvider: &NaiveNodeProvider{}}
	nodes := c.CoreV1().Nodes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := NewNodeController(testP, testNode(t), nodes,
		WithNodeStatusUpdateErrorHandler(func(_ context.Context, err error) error {
			cancel()
			return nil
		}),
	)
	assert.NilError(t, err)

	chErr := make(chan error, 1)
	go func() {
		chErr <- node.Run(ctx)
	}()

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	// wait for the node to be ready
	select {
	case <-timer.C:
		t.Fatal("timeout waiting for node to be ready")
	case <-chErr:
		t.Fatalf("node.Run returned earlier than expected: %v", err)
	case <-node.Ready():
	}

	err = nodes.Delete(node.latestNodeReceivedFromProvider.Name, nil)
	assert.NilError(t, err)

	testP.triggerStatusUpdate(node.latestNodeReceivedFromProvider.DeepCopy())

	timer = time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case err := <-chErr:
		assert.Equal(t, err, nil)
	case <-timer.C:
		t.Fatal("timeout waiting for node shutdown")
	}
}

func TestEnsureLease(t *testing.T) {
	c := testclient.NewSimpleClientset().CoordinationV1beta1().Leases(corev1.NamespaceNodeLease)
	n := testNode(t)
	ctx := context.Background()

	lease := newLease(nil)
	setLeaseAttrs(lease, n, 1*time.Second)

	l1, err := ensureLease(ctx, c, lease.DeepCopy())
	assert.NilError(t, err)
	assert.Check(t, timeEqual(l1.Spec.RenewTime.Time, lease.Spec.RenewTime.Time))

	l1.Spec.RenewTime.Time = time.Now().Add(1 * time.Second)
	l2, err := ensureLease(ctx, c, l1.DeepCopy())
	assert.NilError(t, err)
	assert.Check(t, timeEqual(l2.Spec.RenewTime.Time, l1.Spec.RenewTime.Time))
}

func TestUpdateNodeStatus(t *testing.T) {
	n := testNode(t)
	n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
		LastHeartbeatTime: metav1.Now().Rfc3339Copy(),
	})
	n.Status.Phase = corev1.NodePending
	nodes := testclient.NewSimpleClientset().CoreV1().Nodes()

	ctx := context.Background()
	updated, err := updateNodeStatus(ctx, nodes, nil, n.DeepCopy())
	assert.Equal(t, errors.IsNotFound(err), true, err)

	_, err = nodes.Create(n)
	assert.NilError(t, err)

	updated, err = updateNodeStatus(ctx, nodes, nil, n.DeepCopy())
	assert.NilError(t, err)

	assert.NilError(t, err)
	assert.Check(t, cmp.DeepEqual(n.Status, updated.Status))

	n.Status.Phase = corev1.NodeRunning
	updated, err = updateNodeStatus(ctx, nodes, nil, n.DeepCopy())
	assert.NilError(t, err)
	assert.Check(t, cmp.DeepEqual(n.Status, updated.Status))

	err = nodes.Delete(n.Name, nil)
	assert.NilError(t, err)

	_, err = nodes.Get(n.Name, metav1.GetOptions{})
	assert.Equal(t, errors.IsNotFound(err), true, err)

	_, err = updateNodeStatus(ctx, nodes, nil, updated.DeepCopy())
	assert.Equal(t, errors.IsNotFound(err), true, err)
}

func TestUpdateNodeLease(t *testing.T) {
	leases := testclient.NewSimpleClientset().CoordinationV1beta1().Leases(corev1.NamespaceNodeLease)
	lease := newLease(nil)
	n := testNode(t)
	setLeaseAttrs(lease, n, 0)

	ctx := context.Background()
	l, err := updateNodeLease(ctx, leases, lease)
	assert.NilError(t, err)
	assert.Equal(t, l.Name, lease.Name)
	assert.Assert(t, cmp.DeepEqual(l.Spec.HolderIdentity, lease.Spec.HolderIdentity))

	compare, err := leases.Get(l.Name, emptyGetOptions)
	assert.NilError(t, err)
	assert.Equal(t, l.Spec.RenewTime.Time.Unix(), compare.Spec.RenewTime.Time.Unix())
	assert.Equal(t, compare.Name, lease.Name)
	assert.Assert(t, cmp.DeepEqual(compare.Spec.HolderIdentity, lease.Spec.HolderIdentity))

	l.Spec.RenewTime.Time = time.Now().Add(10 * time.Second)

	compare, err = updateNodeLease(ctx, leases, l.DeepCopy())
	assert.NilError(t, err)
	assert.Equal(t, compare.Spec.RenewTime.Time.Unix(), l.Spec.RenewTime.Time.Unix())
	assert.Equal(t, compare.Name, lease.Name)
	assert.Assert(t, cmp.DeepEqual(compare.Spec.HolderIdentity, lease.Spec.HolderIdentity))
}

// TestPingAfterStatusUpdate checks that Ping continues to be called with the specified interval
// after a node status update occurs, when leases are disabled.
//
// Timing ratios used in this test:
// ping interval (10 ms)
// maximum allowed interval = 2.5 * ping interval
// status update interval = 6 * ping interval
//
// The allowed maximum time is 2.5 times the ping interval because
// the status update resets the ping interval timer, meaning
// that there can be a full two interval durations between
// successive calls to Ping. The extra half is to allow
// for timing variations when using such short durations.
//
// Once the node controller is ready:
// send status update after 10 * ping interval
// end test after another 10 * ping interval
func TestPingAfterStatusUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := testclient.NewSimpleClientset()
	nodes := c.CoreV1().Nodes()

	testP := &testNodeProviderPing{}

	interval := 10 * time.Millisecond
	maxAllowedInterval := time.Duration(2.5 * float64(interval.Nanoseconds()))

	opts := []NodeControllerOpt{
		WithNodePingInterval(interval),
		WithNodeStatusUpdateInterval(interval * time.Duration(6)),
	}

	testNode := testNode(t)
	testNodeCopy := testNode.DeepCopy()

	node, err := NewNodeController(testP, testNode, nodes, opts...)
	assert.NilError(t, err)

	chErr := make(chan error, 1)
	go func() {
		chErr <- node.Run(ctx)
	}()

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	// wait for the node to be ready
	select {
	case <-timer.C:
		t.Fatal("timeout waiting for node to be ready")
	case <-chErr:
		t.Fatalf("node.Run returned earlier than expected: %v", err)
	case <-node.Ready():
	}

	notifyTimer := time.After(interval * time.Duration(10))
	select {
	case <-notifyTimer:
		testP.triggerStatusUpdate(testNodeCopy)
	}

	endTimer := time.After(interval * time.Duration(10))
	select {
	case <-endTimer:
		break
	}

	assert.Assert(t, testP.maxPingInterval < maxAllowedInterval, "maximum time between node pings (%v) was greater than the maximum expected interval (%v)", testP.maxPingInterval, maxAllowedInterval)
}

func testNode(t *testing.T) *corev1.Node {
	n := &corev1.Node{}
	n.Name = strings.ToLower(t.Name())
	return n
}

type testNodeProvider struct {
	NodeProvider
	statusHandlers []func(*corev1.Node)
	// callback status is to let VK know the node, with the updated status
	callbackStatus func(node *corev1.Node)
}

func (p *testNodeProvider) NotifyNodeStatus(ctx context.Context, h func(*corev1.Node)) {
	p.callbackStatus = h
}

func (p *testNodeProvider) triggerStatusUpdate(n *corev1.Node) {
	for _, h := range p.statusHandlers {
		h(n)
	}
	if p.callbackStatus != nil {
		p.callbackStatus(n)
	}
}

// testNodeProviderPing tracks the maximum time interval between calls to Ping
type testNodeProviderPing struct {
	testNodeProvider
	lastPingTime    time.Time
	maxPingInterval time.Duration
}

func (tnp *testNodeProviderPing) Ping(ctx context.Context) error {
	now := time.Now()
	if tnp.lastPingTime.IsZero() {
		tnp.lastPingTime = now
		return nil
	}
	if now.Sub(tnp.lastPingTime) > tnp.maxPingInterval {
		tnp.maxPingInterval = now.Sub(tnp.lastPingTime)
	}
	tnp.lastPingTime = now
	return nil
}

type watchGetter interface {
	Watch(metav1.ListOptions) (watch.Interface, error)
}

func makeWatch(t *testing.T, wc watchGetter, name string) watch.Interface {
	t.Helper()

	w, err := wc.Watch(metav1.ListOptions{FieldSelector: "name=" + name})
	assert.NilError(t, err)
	return w
}

func atLeast(x, atLeast int) cmp.Comparison {
	return func() cmp.Result {
		if x < atLeast {
			return cmp.ResultFailureTemplate(failTemplate("<"), map[string]interface{}{"x": x, "y": atLeast})
		}
		return cmp.ResultSuccess
	}
}

func before(x, y time.Time) cmp.Comparison {
	return func() cmp.Result {
		if x.Before(y) {
			return cmp.ResultSuccess
		}
		return cmp.ResultFailureTemplate(failTemplate(">="), map[string]interface{}{"x": x, "y": y})
	}
}

func timeEqual(x, y time.Time) cmp.Comparison {
	return func() cmp.Result {
		if x.Equal(y) {
			return cmp.ResultSuccess
		}
		return cmp.ResultFailureTemplate(failTemplate("!="), map[string]interface{}{"x": x, "y": y})
	}
}

// waitForEvent waits for the `check` function to return true
// `check` is run when an event is received
// Cancelling the context will cancel the wait, with the context error sent on
// the returned channel.
func waitForEvent(ctx context.Context, chEvent <-chan watch.Event, check func(watch.Event) bool) <-chan error {
	chErr := make(chan error, 1)
	go func() {

		for {
			select {
			case e := <-chEvent:
				if check(e) {
					chErr <- nil
					return
				}
			case <-ctx.Done():
				chErr <- ctx.Err()
				return
			}
		}
	}()

	return chErr
}

func failTemplate(op string) string {
	return `
			{{- .Data.x}} (
				{{- with callArg 0 }}{{ formatNode . }} {{end -}}
				{{- printf "%T" .Data.x -}}
			) ` + op + ` {{ .Data.y}} (
				{{- with callArg 1 }}{{ formatNode . }} {{end -}}
				{{- printf "%T" .Data.y -}}
			)`
}
