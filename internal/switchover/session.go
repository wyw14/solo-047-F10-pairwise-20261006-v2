package switchover

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

type SessionService struct {
	drainer      *route.Drainer
	activator    *route.Activator
	topology     *topology.Registry
	ids          platform.IDGenerator
	now          func() time.Time
	drainTimeout time.Duration

	mu         sync.Mutex
	operations map[string]*trackedOperation
}

// trackedOperation serializes every execution of an operation so repeated or
// concurrent requests never change the topology in parallel.
type trackedOperation struct {
	operation *Operation
	mu        sync.Mutex
}

func NewSessionService(drainer *route.Drainer, activator *route.Activator, registry *topology.Registry, ids platform.IDGenerator, now func() time.Time, drainTimeout time.Duration) *SessionService {
	return &SessionService{
		drainer: drainer, activator: activator, topology: registry,
		ids: ids, now: now, drainTimeout: drainTimeout,
		operations: make(map[string]*trackedOperation),
	}
}

// Failover drains the primary path and activates protection. A drain that
// exceeds the controlled timeout is accepted: protection still carries
// traffic and the operation ends protected-degraded with a drain-timeout
// outcome. Repeated requests for the same route reuse the in-flight or
// finished operation instead of running a second topology change.
func (s *SessionService) Failover(ctx context.Context, target *route.Route) (Snapshot, error) {
	tracked := s.track(target.Snapshot().ID)
	tracked.mu.Lock()
	defer tracked.mu.Unlock()
	if tracked.operation != nil {
		if current := tracked.operation.Snapshot(); current.State != StateCompleted {
			return current, nil
		}
	}
	operation := NewOperation(s.ids.New("switch"), target.Snapshot().ID, s.now())
	tracked.operation = operation
	return s.runFailover(ctx, operation, target)
}

// Retry re-runs the failover flow on the original operation after a failure.
// Operations in any other state are returned unchanged.
func (s *SessionService) Retry(ctx context.Context, target *route.Route) (Snapshot, error) {
	tracked := s.track(target.Snapshot().ID)
	tracked.mu.Lock()
	defer tracked.mu.Unlock()
	if tracked.operation == nil {
		return Snapshot{}, fmt.Errorf("no switchover operation for route %s", target.Snapshot().ID)
	}
	if current := tracked.operation.Snapshot(); current.State != StateFailed {
		return current, nil
	}
	tracked.operation.record(StateCreated, "retry", "retrying failed switchover", s.now())
	return s.runFailover(ctx, tracked.operation, target)
}

// RestorePrimary drains the protection path and reconnects the primary path
// on the original operation, completing the switchover cycle.
func (s *SessionService) RestorePrimary(ctx context.Context, target *route.Route) (Snapshot, error) {
	tracked := s.track(target.Snapshot().ID)
	tracked.mu.Lock()
	defer tracked.mu.Unlock()
	if tracked.operation == nil {
		return Snapshot{}, fmt.Errorf("no switchover operation for route %s", target.Snapshot().ID)
	}
	current := tracked.operation.Snapshot()
	switch current.State {
	case StateProtected, StateProtectedDegraded:
		return s.runRestore(ctx, tracked.operation, target)
	case StateCompleted:
		return current, nil
	default:
		return current, fmt.Errorf("switchover %s is %s, cannot restore primary", current.ID, current.State)
	}
}

func (s *SessionService) track(routeID string) *trackedOperation {
	s.mu.Lock()
	defer s.mu.Unlock()
	tracked := s.operations[routeID]
	if tracked == nil {
		tracked = &trackedOperation{}
		s.operations[routeID] = tracked
	}
	return tracked
}

func (s *SessionService) runFailover(ctx context.Context, operation *Operation, target *route.Route) (Snapshot, error) {
	operation.record(StateDrainingPrimary, "primary-drain", "draining failed primary path", s.now())
	drainTimeout := s.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = s.drainer.Delay()
	}
	drainCtx, cancelDrain := context.WithTimeout(ctx, drainTimeout)
	_, drainErr := s.drainer.Drain(drainCtx, target, s.now())
	cancelDrain()
	drainOutcome := classify(ctx, drainErr)
	if drainOutcome == OutcomeCanceled {
		result := operation.recordOutcome(StateFailed, OutcomeCanceled, "primary-drain", drainErr.Error(), s.now())
		return result, fmt.Errorf("drain primary: %w", drainErr)
	}
	if drainErr != nil {
		operation.recordOutcome(StateDrainingPrimary, drainOutcome, "primary-drain", drainErr.Error(), s.now())
	}
	operation.record(StateActivating, "protection-activate", "activating protection path", s.now())
	if _, err := s.activator.Protection(ctx, target, s.now()); err != nil {
		result := operation.recordOutcome(StateFailed, classify(ctx, err), "protection-activate", err.Error(), s.now())
		return result, fmt.Errorf("activate protection: %w", err)
	}
	switch drainOutcome {
	case OutcomeDrainTimeout:
		return operation.recordOutcome(StateProtectedDegraded, OutcomeDrainTimeout, "complete", "protection path carries traffic after primary drain timeout", s.now()), nil
	case OutcomeDeviceFailure:
		return operation.recordOutcome(StateProtectedDegraded, OutcomeDeviceFailure, "complete", "protection path carries traffic after primary drain failure", s.now()), nil
	default:
		return operation.record(StateProtected, "complete", "protection path carries traffic", s.now()), nil
	}
}

func (s *SessionService) runRestore(ctx context.Context, operation *Operation, target *route.Route) (Snapshot, error) {
	operation.record(StateRestoring, "protection-drain", "draining protection traffic", s.now())
	if _, err := s.drainer.Drain(ctx, target, s.now()); err != nil {
		result := operation.recordOutcome(StateFailed, classify(ctx, err), "protection-drain", err.Error(), s.now())
		return result, err
	}
	if _, err := s.topology.Open(target.Snapshot().SpanID, topology.PathProtection, s.now()); err != nil {
		result := operation.recordOutcome(StateFailed, OutcomeDeviceFailure, "protection-open", err.Error(), s.now())
		return result, err
	}
	operation.record(StateRestoring, "primary-connect", "connecting recovered primary path", s.now())
	if _, err := s.activator.Primary(ctx, target, s.now()); err != nil {
		result := operation.recordOutcome(StateFailed, classify(ctx, err), "primary-connect", err.Error(), s.now())
		return result, fmt.Errorf("connect primary: %w", err)
	}
	return operation.record(StateCompleted, "complete", "primary route restored", s.now()), nil
}

// classify maps a stage error to an outcome. A deadline exceeded while the
// caller context is still alive is the controlled drain timeout, which is
// acceptable; a dead caller context means the request was canceled.
func classify(ctx context.Context, err error) Outcome {
	switch {
	case err == nil:
		return OutcomeNone
	case errors.Is(err, context.Canceled):
		return OutcomeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		if ctx.Err() != nil {
			return OutcomeCanceled
		}
		return OutcomeDrainTimeout
	default:
		return OutcomeDeviceFailure
	}
}
