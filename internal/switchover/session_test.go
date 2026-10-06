package switchover

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

var testNow = time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)

func newService(t *testing.T, registry *topology.Registry, drainDelay, drainTimeout time.Duration) *SessionService {
	t.Helper()
	drainer := route.NewDrainer(drainDelay)
	activator := route.NewActivator(registry)
	return NewSessionService(drainer, activator, registry, platform.UUIDGenerator{}, func() time.Time { return testNow }, drainTimeout)
}

func newRoute(t *testing.T, routeID, spanID string) *route.Route {
	t.Helper()
	target, err := route.New(routeID, spanID, testNow)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func registeredTopology(t *testing.T, spanID string) *topology.Registry {
	t.Helper()
	registry := topology.NewRegistry()
	if _, err := registry.Register(spanID, topology.PathPrimary, testNow); err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestDrainTimeoutIsAcceptedAndProtectsDegraded(t *testing.T) {
	registry := registeredTopology(t, "S3")
	service := newService(t, registry, 50*time.Millisecond, 5*time.Millisecond)
	target := newRoute(t, "R-S3", "S3")

	snapshot, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != StateProtectedDegraded || snapshot.Outcome != OutcomeDrainTimeout {
		t.Fatalf("expected protected-degraded with drain-timeout outcome, got %+v", snapshot)
	}
	if active := target.Snapshot().ActivePath; active != topology.PathProtection {
		t.Fatalf("protection path must carry traffic, active=%s", active)
	}
}

func TestDeviceFailureIsClassified(t *testing.T) {
	registry := topology.NewRegistry() // span S9 is never registered
	service := newService(t, registry, 0, time.Second)
	target := newRoute(t, "R-S9", "S9")

	snapshot, err := service.Failover(context.Background(), target)
	if err == nil {
		t.Fatal("unregistered span must fail protection activation")
	}
	if snapshot.State != StateFailed || snapshot.Outcome != OutcomeDeviceFailure {
		t.Fatalf("expected failed with device-failure outcome, got %+v", snapshot)
	}
}

func TestContextCancellationIsClassifiedAndDoesNotActivate(t *testing.T) {
	registry := registeredTopology(t, "S3")
	service := newService(t, registry, 50*time.Millisecond, time.Minute)
	target := newRoute(t, "R-S3", "S3")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	snapshot, err := service.Failover(ctx, target)
	if err == nil {
		t.Fatal("canceled context must fail the operation")
	}
	if snapshot.State != StateFailed || snapshot.Outcome != OutcomeCanceled {
		t.Fatalf("expected failed with canceled outcome, got %+v", snapshot)
	}
	topologySnapshot, err := registry.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if topologySnapshot.Generation != 1 || topologySnapshot.ActivePath != topology.PathPrimary {
		t.Fatalf("canceled operation must not change topology, got %+v", topologySnapshot)
	}
}

func TestRepeatedFailoverReusesOperation(t *testing.T) {
	registry := registeredTopology(t, "S3")
	service := newService(t, registry, 0, time.Second)
	target := newRoute(t, "R-S3", "S3")

	first, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("repeated request must reuse operation %s, got %s", first.ID, second.ID)
	}
	if second.State != StateProtected {
		t.Fatalf("reused operation must keep its terminal state, got %+v", second)
	}
	if generation, err := registry.Generation("S3"); err != nil || generation != 3 {
		t.Fatalf("topology must change exactly once, generation=%d err=%v", generation, err)
	}
}

func TestConcurrentFailoverNeverForksTopology(t *testing.T) {
	registry := registeredTopology(t, "S3")
	service := newService(t, registry, 20*time.Millisecond, time.Second)
	target := newRoute(t, "R-S3", "S3")

	const callers = 8
	ids := make([]string, callers)
	var wait sync.WaitGroup
	for index := range ids {
		wait.Add(1)
		go func(slot int) {
			defer wait.Done()
			snapshot, err := service.Failover(context.Background(), target)
			if err != nil {
				t.Error(err)
				return
			}
			ids[slot] = snapshot.ID
		}(index)
	}
	wait.Wait()
	for index := 1; index < callers; index++ {
		if ids[index] != ids[0] {
			t.Fatalf("concurrent requests must share one operation, ids=%v", ids)
		}
	}
	if generation, err := registry.Generation("S3"); err != nil || generation != 3 {
		t.Fatalf("parallel requests must not fork topology, generation=%d err=%v", generation, err)
	}
}

func TestRetryRerunsFailedOperation(t *testing.T) {
	registry := topology.NewRegistry()
	service := newService(t, registry, 0, time.Second)
	target := newRoute(t, "R-S9", "S9")

	failed, err := service.Failover(context.Background(), target)
	if err == nil || failed.Outcome != OutcomeDeviceFailure {
		t.Fatalf("expected device failure, got %+v err=%v", failed, err)
	}
	if _, err := registry.Register("S9", topology.PathPrimary, testNow); err != nil {
		t.Fatal(err)
	}
	retried, err := service.Retry(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if retried.ID != failed.ID {
		t.Fatalf("retry must reuse operation %s, got %s", failed.ID, retried.ID)
	}
	if retried.State != StateProtected {
		t.Fatalf("retry must protect the route, got %+v", retried)
	}
}

func TestRetryKeepsProtectedOperationUntouched(t *testing.T) {
	registry := registeredTopology(t, "S3")
	service := newService(t, registry, 0, time.Second)
	target := newRoute(t, "R-S3", "S3")

	protected, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := service.Retry(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if retried.ID != protected.ID || retried.State != StateProtected {
		t.Fatalf("retry on a protected operation must be a no-op, got %+v", retried)
	}
	if generation, err := registry.Generation("S3"); err != nil || generation != 3 {
		t.Fatalf("retry must not change topology again, generation=%d err=%v", generation, err)
	}
}

func TestRestorePrimaryCompletesCycle(t *testing.T) {
	registry := registeredTopology(t, "S3")
	service := newService(t, registry, 0, time.Second)
	target := newRoute(t, "R-S3", "S3")

	protected, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := service.RestorePrimary(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if restored.ID != protected.ID || restored.State != StateCompleted {
		t.Fatalf("restore must complete the original operation, got %+v", restored)
	}
	if active := target.Snapshot().ActivePath; active != topology.PathPrimary {
		t.Fatalf("primary path must carry traffic again, active=%s", active)
	}

	next, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID == restored.ID {
		t.Fatal("a completed cycle must allow a fresh operation")
	}
}

func TestRestorePrimaryRequiresProtection(t *testing.T) {
	registry := registeredTopology(t, "S3")
	service := newService(t, registry, 0, time.Second)
	target := newRoute(t, "R-S3", "S3")

	if _, err := service.RestorePrimary(context.Background(), target); err == nil {
		t.Fatal("restore without a protected operation must fail")
	}
}
