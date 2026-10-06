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

// pathDrainer 抽象主/保护路径排空，具体实现为 *route.Drainer。
type pathDrainer interface {
	Drain(ctx context.Context, target *route.Route, at time.Time) (route.Snapshot, error)
	Delay() time.Duration
}

// pathActivator 抽象保护/主路径激活，具体实现为 *route.Activator。
type pathActivator interface {
	Protection(ctx context.Context, target *route.Route, at time.Time) (route.Snapshot, error)
	Primary(ctx context.Context, target *route.Route, at time.Time) (route.Snapshot, error)
}

// Manager 串行化同一线路上的所有切换/恢复操作：
//   - 同类型的重复请求复用原操作（执行中则等待其结束），绝不并行改变拓扑；
//   - 不同类型的并发请求返回 ConflictError；
//   - 区分可接受的主路径排空超时、设备失败与上下文取消；
//   - 暴露受控的 Retry / Restore 入口。
type Manager struct {
	drainer      pathDrainer
	activator    pathActivator
	topology     *topology.Registry
	ids          platform.IDGenerator
	now          func() time.Time
	drainTimeout time.Duration

	mu         sync.Mutex
	routes     map[string]*routeState
	operations map[string]*Operation
}

type routeState struct {
	active   *Operation
	last     *Operation
	attempt  int
	lastKind Kind
	wait     chan struct{}
}

func NewManager(drainer pathDrainer, activator pathActivator, registry *topology.Registry, ids platform.IDGenerator, now func() time.Time, drainTimeout time.Duration) *Manager {
	return &Manager{
		drainer:      drainer,
		activator:    activator,
		topology:     registry,
		ids:          ids,
		now:          now,
		drainTimeout: drainTimeout,
		routes:       make(map[string]*routeState),
		operations:   make(map[string]*Operation),
	}
}

var (
	// ErrAlreadyPrimary 表示路由已承载在主路径上，恢复主路径是空操作。
	// 调用方（如验收流程）可将其视为"已恢复"。
	ErrAlreadyPrimary = errors.New("route already carries traffic on its primary path")
)

// Failover 在 target 上发起主路径 -> 保护路径切换。
// 重复的同类请求复用原操作：执行中则等待，已结束则返回原操作快照。
func (m *Manager) Failover(ctx context.Context, target *route.Route) (Snapshot, error) {
	routeID := target.Snapshot().ID

	m.mu.Lock()
	state := m.route(routeID)
	if active := state.active; active != nil {
		if snap := active.Snapshot(); snap.Kind == KindFailover {
			return m.awaitLocked(ctx, state, active)
		} else {
			snap := active.Snapshot()
			m.mu.Unlock()
			return snap, ConflictError{Detail: fmt.Sprintf("operation %s (%s) is in progress on route %s", snap.ID, snap.Kind, routeID)}
		}
	}
	if state.last != nil && state.last.Snapshot().Kind == KindFailover {
		last := state.last.Snapshot()
		m.mu.Unlock()
		// 切换已发生过：复用原记录，要求重试或恢复必须走受控入口。
		return last, DuplicateRequest{Active: false, OperationID: last.ID}
	}
	op, done := m.beginLocked(state, KindFailover, routeID, "")
	m.mu.Unlock()

	result, err := m.runFailover(ctx, op, target)
	m.finish(state, op, done)
	return result, err
}

// Retry 重新执行一个仍可重试的终态操作（排空超时、设备失败或取消）。
func (m *Manager) Retry(ctx context.Context, operationID string, target *route.Route) (Snapshot, error) {
	routeID := target.Snapshot().ID

	m.mu.Lock()
	op := m.operations[operationID]
	if op == nil || op.Snapshot().RouteID != routeID {
		m.mu.Unlock()
		return Snapshot{}, ConflictError{Detail: fmt.Sprintf("operation %s not found for route %s", operationID, routeID)}
	}
	state := m.routes[routeID]
	if active := state.active; active != nil {
		snap := active.Snapshot()
		// 重试的目标操作正在执行，或同类型操作已在途：复用原操作，不另起拓扑变更。
		if active == op || snap.Kind == op.Snapshot().Kind {
			return m.awaitLocked(ctx, state, active)
		}
		m.mu.Unlock()
		return snap, ConflictError{Detail: fmt.Sprintf("operation %s (%s) is in progress on route %s", snap.ID, snap.Kind, routeID)}
	}
	if state.last != op {
		m.mu.Unlock()
		return Snapshot{}, ConflictError{Detail: fmt.Sprintf("operation %s has been superseded; retry the latest operation", operationID)}
	}
	snap := op.Snapshot()
	if !snap.Retriable {
		m.mu.Unlock()
		return snap, ConflictError{Detail: fmt.Sprintf("operation %s is not retriable in state %s", operationID, snap.State)}
	}
	kind := snap.Kind
	next, done := m.beginLocked(state, kind, routeID, operationID)
	m.mu.Unlock()

	var (
		result Snapshot
		err    error
	)
	if kind == KindRestore {
		result, err = m.runRestore(ctx, next, target)
	} else {
		result, err = m.runFailover(ctx, next, target)
	}
	m.finish(state, next, done)
	return result, err
}

// Restore 是受控的"恢复主路径"入口。只有业务确实承载在保护路径上
// （切换成功或排空超时后）才执行拓扑变更；已在主路径上时返回 ErrAlreadyPrimary。
// 恢复操作执行期间的重复请求复用原操作。
func (m *Manager) Restore(ctx context.Context, target *route.Route) (Snapshot, error) {
	routeID := target.Snapshot().ID

	m.mu.Lock()
	state := m.route(routeID)
	if active := state.active; active != nil {
		if active.Snapshot().Kind == KindRestore {
			return m.awaitLocked(ctx, state, active)
		}
		snap := active.Snapshot()
		m.mu.Unlock()
		return snap, ConflictError{Detail: fmt.Sprintf("operation %s (%s) is in progress on route %s", snap.ID, snap.Kind, routeID)}
	}
	if state.last != nil {
		last := state.last.Snapshot()
		if last.Kind == KindRestore {
			switch {
			case last.State == StateCompleted:
				m.mu.Unlock()
				return last, DuplicateRequest{Active: false, OperationID: last.ID}
			case last.Retriable:
				// 上次恢复失败/取消：重复的恢复请求沿用 attempt 链路重新执行。
				next, done := m.beginLocked(state, KindRestore, routeID, last.ID)
				m.mu.Unlock()
				result, err := m.runRestore(ctx, next, target)
				m.finish(state, next, done)
				return result, err
			}
		}
	}
	// 持锁检查路径，防止与 begin 之间的 TOCTOU 竞争。
	if target.Snapshot().ActivePath != topology.PathProtection {
		m.mu.Unlock()
		return Snapshot{}, ErrAlreadyPrimary
	}
	op, done := m.beginLocked(state, KindRestore, routeID, "")
	m.mu.Unlock()

	result, err := m.runRestore(ctx, op, target)
	m.finish(state, op, done)
	return result, err
}

// Latest 返回该线路最近一个操作的快照。
func (m *Manager) Latest(routeID string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.routes[routeID]
	if state == nil {
		return Snapshot{}, false
	}
	if state.active != nil {
		return state.active.Snapshot(), true
	}
	if state.last != nil {
		return state.last.Snapshot(), true
	}
	return Snapshot{}, false
}

// Operation 按 ID 返回操作快照。
func (m *Manager) Operation(operationID string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	op := m.operations[operationID]
	if op == nil {
		return Snapshot{}, false
	}
	return op.Snapshot(), true
}

func (m *Manager) route(routeID string) *routeState {
	state := m.routes[routeID]
	if state == nil {
		state = &routeState{}
		m.routes[routeID] = state
	}
	return state
}

// beginLocked 登记一个新操作。调用方必须持有 m.mu。
func (m *Manager) beginLocked(state *routeState, kind Kind, routeID, retryOf string) (*Operation, chan struct{}) {
	if state.lastKind == kind {
		state.attempt++
	} else {
		state.attempt = 1
	}
	op := NewOperation(m.ids.New(string(kind)), routeID, kind, state.attempt, retryOf, m.now())
	state.active = op
	state.lastKind = kind
	state.wait = make(chan struct{})
	m.operations[op.Snapshot().ID] = op
	return op, state.wait
}

func (m *Manager) finish(state *routeState, op *Operation, done chan struct{}) {
	m.mu.Lock()
	if state.active == op {
		state.active = nil
		state.last = op
		close(done)
	}
	m.mu.Unlock()
}

// awaitLocked 复用一个执行中的同类型操作，等待其结束。调用方的 ctx 取消不影响原操作。
func (m *Manager) awaitLocked(ctx context.Context, state *routeState, active *Operation) (Snapshot, error) {
	wait := state.wait
	snap := active.Snapshot()
	m.mu.Unlock()
	select {
	case <-wait:
	case <-ctx.Done():
		return Snapshot{}, fmt.Errorf("await operation %s: %w", snap.ID, ctx.Err())
	}
	result := active.Snapshot()
	return result, DuplicateRequest{Active: true, OperationID: snap.ID}
}

func (m *Manager) drainGrace() time.Duration {
	if m.drainTimeout > 0 {
		return m.drainTimeout
	}
	return m.drainer.Delay()
}

func (m *Manager) runFailover(ctx context.Context, op *Operation, target *route.Route) (Snapshot, error) {
	op.record(StateDrainingPrimary, "primary-drain", "draining failed primary path", m.now())

	drainTimedOut := false
	drainCtx, cancelDrain := context.WithTimeout(ctx, m.drainGrace())
	_, drainErr := m.drainer.Drain(drainCtx, target, m.now())
	cancelDrain()
	switch {
	case drainErr == nil:
	case ctx.Err() != nil:
		// 调用方取消优先：宽限时间恰好到点也不再继续，避免在无人接管时改变拓扑。
		result := op.finish(StateCanceled, OutcomeCanceled, "primary-drain", "failover canceled while draining primary: "+ctx.Err().Error(), m.now())
		return result, fmt.Errorf("failover canceled: %w", ctx.Err())
	case errors.Is(drainErr, context.DeadlineExceeded):
		// 宽限时间到、调用方仍在：可接受的排空超时，继续把业务切到保护路径。
		drainTimedOut = true
		op.record(StateDrainingPrimary, "primary-drain-timeout", "primary drain exceeded grace period; continuing to protection path", m.now())
	default:
		result := op.finish(StateFailed, OutcomeDeviceFailure, "primary-drain", drainErr.Error(), m.now())
		return result, newDeviceFailure("drain primary", drainErr)
	}

	op.record(StateActivating, "protection-activate", "activating protection path", m.now())
	if _, err := m.activator.Protection(ctx, target, m.now()); err != nil {
		if ctx.Err() != nil {
			result := op.finish(StateCanceled, OutcomeCanceled, "protection-activate", "failover canceled while activating protection: "+ctx.Err().Error(), m.now())
			return result, fmt.Errorf("failover canceled: %w", ctx.Err())
		}
		result := op.finish(StateFailed, OutcomeDeviceFailure, "protection-activate", err.Error(), m.now())
		return result, newDeviceFailure("protection-activate", err)
	}

	if drainTimedOut {
		return op.finish(StateProtected, OutcomeDrainTimeout, "complete", "protection path carries traffic after primary drain timeout", m.now()), nil
	}
	return op.finish(StateProtected, OutcomeNone, "complete", "protection path carries traffic", m.now()), nil
}

func (m *Manager) runRestore(ctx context.Context, op *Operation, target *route.Route) (Snapshot, error) {
	op.record(StateRestoring, "protection-drain", "draining protection traffic", m.now())
	if _, err := m.drainer.Drain(ctx, target, m.now()); err != nil {
		if ctx.Err() != nil {
			result := op.finish(StateCanceled, OutcomeCanceled, "protection-drain", "restore canceled while draining protection: "+ctx.Err().Error(), m.now())
			return result, fmt.Errorf("restore canceled: %w", ctx.Err())
		}
		result := op.finish(StateFailed, OutcomeDeviceFailure, "protection-drain", err.Error(), m.now())
		return result, newDeviceFailure("drain protection", err)
	}
	if _, err := m.topology.Open(target.Snapshot().SpanID, topology.PathProtection, m.now()); err != nil {
		result := op.finish(StateFailed, OutcomeDeviceFailure, "protection-open", err.Error(), m.now())
		return result, newDeviceFailure("protection-open", err)
	}
	op.record(StateRestoring, "primary-connect", "connecting recovered primary path", m.now())
	if _, err := m.activator.Primary(ctx, target, m.now()); err != nil {
		if ctx.Err() != nil {
			result := op.finish(StateCanceled, OutcomeCanceled, "primary-connect", "restore canceled while connecting primary: "+ctx.Err().Error(), m.now())
			return result, fmt.Errorf("restore canceled: %w", ctx.Err())
		}
		result := op.finish(StateFailed, OutcomeDeviceFailure, "primary-connect", err.Error(), m.now())
		return result, newDeviceFailure("primary-connect", err)
	}
	return op.finish(StateCompleted, OutcomeNone, "complete", "primary route restored", m.now()), nil
}
