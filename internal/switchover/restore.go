package switchover

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

type Restorer struct {
	drainer   *route.Drainer
	activator *route.Activator
	topology  *topology.Registry
	ids       platform.IDGenerator
	now       func() time.Time
}

func NewRestorer(drainer *route.Drainer, activator *route.Activator, registry *topology.Registry, ids platform.IDGenerator, now func() time.Time) *Restorer {
	return &Restorer{drainer: drainer, activator: activator, topology: registry, ids: ids, now: now}
}

func (r *Restorer) Restore(ctx context.Context, target *route.Route) (Snapshot, error) {
	operation := NewOperation(r.ids.New("restore"), target.Snapshot().ID, r.now())
	operation.record(StateRestoring, "protection-drain", "draining protection traffic", r.now())
	if _, err := r.drainer.Drain(ctx, target, r.now()); err != nil {
		result := operation.recordOutcome(StateFailed, classify(ctx, err), "protection-drain", err.Error(), r.now())
		return result, err
	}
	if _, err := r.topology.Open(target.Snapshot().SpanID, topology.PathProtection, r.now()); err != nil {
		result := operation.recordOutcome(StateFailed, OutcomeDeviceFailure, "protection-open", err.Error(), r.now())
		return result, err
	}
	operation.record(StateRestoring, "primary-connect", "connecting recovered primary path", r.now())
	if _, err := r.activator.Primary(ctx, target, r.now()); err != nil {
		result := operation.recordOutcome(StateFailed, classify(ctx, err), "primary-connect", err.Error(), r.now())
		return result, fmt.Errorf("connect primary: %w", err)
	}
	return operation.record(StateCompleted, "complete", "primary route restored", r.now()), nil
}
