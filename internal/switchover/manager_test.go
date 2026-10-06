package switchover

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

type scriptClock struct {
	base time.Time
	n    atomic.Int64
}

func (c *scriptClock) now() time.Time {
	return c.base.Add(time.Duration(c.n.Add(1)) * time.Second)
}

type seqIDs struct{ n atomic.Int64 }

func (s *seqIDs) New(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, s.n.Add(1))
}

// blockingDrainer 可以阻塞 Drain 直到 release 关闭，或直接返回设备错误。
type blockingDrainer struct {
	delay    time.Duration
	release  chan struct{}
	started  chan struct{}
	failWith error
	calls    atomic.Int32
}

func newBlockingDrainer(release chan struct{}) *blockingDrainer {
	return &blockingDrainer{release: release, started: make(chan struct{}, 8)}
}

func (d *blockingDrainer) Drain(ctx context.Context, _ *route.Route, _ time.Time) (route.Snapshot, error) {
	d.calls.Add(1)
	select {
	case d.started <- struct{}{}:
	default:
	}
	if d.failWith != nil {
		return route.Snapshot{}, d.failWith
	}
	select {
	case <-ctx.Done():
		return route.Snapshot{}, fmt.Errorf("route drain interrupted: %w", ctx.Err())
	case <-d.release:
	}
	return route.Snapshot{}, nil
}

func (d *blockingDrainer) Delay() time.Duration { return d.delay }

type fakeActivator struct {
	real            *route.Activator
	failProtection  atomic.Bool
	protectionCalls atomic.Int32
}

func (a *fakeActivator) Protection(ctx context.Context, target *route.Route, at time.Time) (route.Snapshot, error) {
	a.protectionCalls.Add(1)
	if a.failProtection.Load() {
		return target.Snapshot(), errors.New("protection cross-connect rack offline")
	}
	return a.real.Protection(ctx, target, at)
}

func (a *fakeActivator) Primary(ctx context.Context, target *route.Route, at time.Time) (route.Snapshot, error) {
	return a.real.Primary(ctx, target, at)
}

type harness struct {
	manager   *Manager
	target    *route.Route
	drainer   *blockingDrainer
	activator *fakeActivator
}

func newHarness(t *testing.T, grace time.Duration, release chan struct{}) *harness {
	t.Helper()
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	target, err := route.New("R-S3", "S3", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	drainer := newBlockingDrainer(release)
	activator := &fakeActivator{real: route.NewActivator(registry)}
	clock := &scriptClock{base: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	return &harness{
		manager:   NewManager(drainer, activator, registry, &seqIDs{}, clock.now, grace),
		target:    target,
		drainer:   drainer,
		activator: activator,
	}
}

func TestFailoverSuccessMarksProtectedAndRestorable(t *testing.T) {
	release := make(chan struct{})
	close(release)
	h := newHarness(t, time.Second, release)

	result, err := h.manager.Failover(context.Background(), h.target)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}
	if result.State != StateProtected || result.DrainOutcome != OutcomeNone {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !result.Restorable || result.Retriable {
		t.Fatalf("protected snapshot should be restorable and not retriable: %+v", result)
	}
	if result.Kind != KindFailover || result.Attempt != 1 || result.ID == "" {
		t.Fatalf("unexpected operation metadata: %+v", result)
	}
	if got := h.target.Snapshot(); got.ActivePath != topology.PathProtection {
		t.Fatalf("route should carry protection traffic, got %+v", got)
	}
	if latest, ok := h.manager.Latest("R-S3"); !ok || latest.ID != result.ID {
		t.Fatalf("latest lookup mismatch: %+v ok=%v", latest, ok)
	}
}

func TestFailoverDrainTimeoutIsAcceptableAndClassified(t *testing.T) {
	h := newHarness(t, 20*time.Millisecond, make(chan struct{}))

	result, err := h.manager.Failover(context.Background(), h.target)
	<-h.drainer.started
	if err != nil {
		t.Fatalf("failover after drain timeout must succeed on protection: %v", err)
	}
	if result.State != StateProtected || result.DrainOutcome != OutcomeDrainTimeout {
		t.Fatalf("expected protected+drain-timeout, got state=%s outcome=%s", result.State, result.DrainOutcome)
	}
	if !result.DrainTimedOut() || !result.Retriable || !result.Restorable {
		t.Fatalf("drain-timeout snapshot flags wrong: %+v", result)
	}
	if got := h.target.Snapshot(); got.ActivePath != topology.PathProtection {
		t.Fatalf("route should have moved to protection path, got %s", got.ActivePath)
	}
	if h.drainer.calls.Load() != 1 {
		t.Fatalf("expected exactly one drain attempt, got %d", h.drainer.calls.Load())
	}
}

func TestFailoverDeviceFailureIsClassifiedAndRetriable(t *testing.T) {
	release := make(chan struct{})
	close(release)
	h := newHarness(t, time.Second, release)
	h.activator.failProtection.Store(true)

	result, err := h.manager.Failover(context.Background(), h.target)
	if err == nil {
		t.Fatal("expected device failure error")
	}
	var device *DeviceFailureError
	if !errors.As(err, &device) || device.Stage != "protection-activate" {
		t.Fatalf("expected DeviceFailureError at activation, got %v", err)
	}
	if result.State != StateFailed || result.DrainOutcome != OutcomeDeviceFailure || !result.Retriable || result.Restorable {
		t.Fatalf("unexpected failure snapshot: %+v", result)
	}
	if got := h.target.Snapshot(); got.ActivePath != topology.PathPrimary {
		t.Fatalf("failed activation must not move traffic, got %s", got.ActivePath)
	}

	// 受控重试：修复设备后重新执行同一链路。
	h.activator.failProtection.Store(false)
	retry, err := h.manager.Retry(context.Background(), result.ID, h.target)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.State != StateProtected || retry.Attempt != 2 || retry.RetryOf != result.ID {
		t.Fatalf("unexpected retry snapshot: %+v", retry)
	}
}

func TestFailoverContextCancelIsClassifiedAndDoesNotTouchTopology(t *testing.T) {
	release := make(chan struct{})
	h := newHarness(t, time.Second, release)

	ctx, cancel := context.WithCancel(context.Background())
	var result Snapshot
	var err error
	done := make(chan struct{})
	go func() {
		result, err = h.manager.Failover(ctx, h.target)
		close(done)
	}()
	<-h.drainer.started
	cancel()
	<-done

	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if result.State != StateCanceled || result.DrainOutcome != OutcomeCanceled || !result.Retriable {
		t.Fatalf("unexpected canceled snapshot: %+v", result)
	}
	if got := h.target.Snapshot(); got.ActivePath != topology.PathPrimary {
		t.Fatalf("canceled failover must leave traffic on primary, got %s", got.ActivePath)
	}
	if h.activator.protectionCalls.Load() != 0 {
		t.Fatal("protection must never activate after cancellation")
	}

	// 取消后受控重试应成功。
	close(release)
	retry, retryErr := h.manager.Retry(context.Background(), result.ID, h.target)
	if retryErr != nil {
		t.Fatalf("retry after cancel: %v", retryErr)
	}
	if retry.State != StateProtected || retry.Attempt != 2 {
		t.Fatalf("unexpected retry result: %+v", retry)
	}
}

func TestConcurrentFailoverRequestsReuseOneOperation(t *testing.T) {
	release := make(chan struct{})
	h := newHarness(t, time.Second, release)

	var (
		first, second     Snapshot
		firstErr, secondE error
		wg                sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		first, firstErr = h.manager.Failover(context.Background(), h.target)
	}()
	<-h.drainer.started

	wg.Add(1)
	go func() {
		defer wg.Done()
		second, secondE = h.manager.Failover(context.Background(), h.target)
	}()
	// 给第二个请求进入等待状态留出调度时间。
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if firstErr != nil || first.State != StateProtected {
		t.Fatalf("first failover: %+v err=%v", first, firstErr)
	}
	var duplicate DuplicateRequest
	if !errors.As(secondE, &duplicate) || !duplicate.Active || duplicate.OperationID != first.ID {
		t.Fatalf("second request must reuse active operation, got %+v err=%v", second, secondE)
	}
	if second.ID != first.ID || h.drainer.calls.Load() != 1 {
		t.Fatalf("topology changed in parallel: calls=%d ids=%s/%s", h.drainer.calls.Load(), first.ID, second.ID)
	}
}

func TestCrossTypeRequestsConflictWhileOperationActive(t *testing.T) {
	release := make(chan struct{})
	h := newHarness(t, time.Second, release)

	go func() {
		_, _ = h.manager.Failover(context.Background(), h.target)
	}()
	<-h.drainer.started

	if _, err := h.manager.Restore(context.Background(), h.target); !errors.As(err, new(ConflictError)) {
		t.Fatalf("restore during failover must conflict, got %v", err)
	}
	close(release)
}

func TestRepeatedFailoverAfterTerminalReusesRecord(t *testing.T) {
	release := make(chan struct{})
	close(release)
	h := newHarness(t, time.Second, release)

	first, err := h.manager.Failover(context.Background(), h.target)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.manager.Failover(context.Background(), h.target)
	var duplicate DuplicateRequest
	if !errors.As(err, &duplicate) || duplicate.Active || duplicate.OperationID != first.ID {
		t.Fatalf("terminal duplicate must return inactive DuplicateRequest, got %+v err=%v", second, err)
	}
	if second.ID != first.ID {
		t.Fatal("duplicate request must reuse original operation id")
	}
}

func TestRestoreFlowAndGuards(t *testing.T) {
	release := make(chan struct{})
	close(release)
	h := newHarness(t, time.Second, release)

	// 未切换前恢复主路径被拒绝。
	if _, err := h.manager.Restore(context.Background(), h.target); !errors.Is(err, ErrAlreadyPrimary) {
		t.Fatalf("restore on primary must be ErrAlreadyPrimary, got %v", err)
	}

	protected, err := h.manager.Failover(context.Background(), h.target)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := h.manager.Restore(context.Background(), h.target)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.State != StateCompleted || restored.Kind != KindRestore || restored.Attempt != 1 {
		t.Fatalf("unexpected restore snapshot: %+v", restored)
	}
	if got := h.target.Snapshot(); got.ActivePath != topology.PathPrimary {
		t.Fatalf("traffic should be back on primary, got %s", got.ActivePath)
	}

	// 重复恢复复用已完成记录。
	again, err := h.manager.Restore(context.Background(), h.target)
	var duplicate DuplicateRequest
	if !errors.As(err, &duplicate) || duplicate.OperationID != restored.ID || again.ID != restored.ID {
		t.Fatalf("duplicate restore must reuse completed operation, got %+v err=%v", again, err)
	}

	// 非可重试操作不允许 Retry。
	if _, err := h.manager.Retry(context.Background(), protected.ID, h.target); !errors.As(err, new(ConflictError)) {
		t.Fatalf("retrying a non-retriable operation must conflict, got %v", err)
	}
}

func TestRetryUnknownOperationConflicts(t *testing.T) {
	release := make(chan struct{})
	close(release)
	h := newHarness(t, time.Second, release)

	if _, err := h.manager.Retry(context.Background(), "failover-missing", h.target); !errors.As(err, new(ConflictError)) {
		t.Fatalf("unknown retry must conflict, got %v", err)
	}
	if _, ok := h.manager.Operation("failover-missing"); ok {
		t.Fatal("unknown operation must not exist")
	}
}
