package switchover

import (
	"sync"
	"time"
)

type State string

const (
	StateCreated         State = "created"
	StateDrainingPrimary State = "draining-primary"
	StateActivating      State = "activating-protection"
	StateProtected       State = "protected"
	StateRestoring       State = "restoring-primary"
	StateCompleted       State = "completed"
	StateFailed          State = "failed"
	StateCanceled        State = "canceled"
)

// Outcome 分类一次切换操作的非成功结束方式。空字符串表示尚未结束或成功结束。
type Outcome string

const (
	// OutcomeNone 表示操作仍在进行，或成功结束。
	OutcomeNone Outcome = ""
	// OutcomeDrainTimeout 主路径排空达到宽限时间但仍成功切到保护路径，可接受。
	OutcomeDrainTimeout Outcome = "drain-timeout"
	// OutcomeDeviceFailure 设备侧动作失败（排空硬件报错或保护路径激活失败）。
	OutcomeDeviceFailure Outcome = "device-failure"
	// OutcomeCanceled 调用方上下文取消（含客户端断连），操作在受控边界中止。
	OutcomeCanceled Outcome = "canceled"
)

// Kind 标记操作的业务方向。
type Kind string

const (
	KindFailover Kind = "failover"
	KindRestore  Kind = "restore"
)

type Event struct {
	Stage   string    `json:"stage"`
	Outcome Outcome   `json:"outcome,omitempty"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}

type Snapshot struct {
	ID      string `json:"id"`
	RouteID string `json:"route_id"`
	Kind    Kind   `json:"kind"`
	State   State  `json:"state"`
	// DrainOutcome 标注主路径排空的结果分类；成功结束且无异常时为空。
	DrainOutcome Outcome `json:"drain_outcome,omitempty"`
	// Retriable 为 true 时调用方可通过 Retry 入口重新执行同一操作。
	Retriable bool `json:"retriable,omitempty"`
	// Restorable 为 true 时表示当前处于保护路径，可发起恢复主路径。
	Restorable bool `json:"restorable,omitempty"`
	// RetryOf 非空时记录本操作是哪个操作的重试。
	RetryOf string `json:"retry_of,omitempty"`
	// Attempt 记录这是该链路第几次执行（首次为 1）。
	Attempt   int       `json:"attempt"`
	Events    []Event   `json:"events"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Terminal 报告操作是否已进入终态，不能再被同一条记录推进。
func (s Snapshot) Terminal() bool {
	switch s.State {
	case StateProtected, StateCompleted, StateFailed, StateCanceled:
		return true
	}
	return false
}

// InProgress 报告操作是否仍在执行。
func (s Snapshot) InProgress() bool { return !s.Terminal() }

// DrainTimedOut 报告主路径排空是否走了"超时但继续保护"的可接受路径。
func (s Snapshot) DrainTimedOut() bool { return s.DrainOutcome == OutcomeDrainTimeout }

// DuplicateRequest 标记重复请求复用了原操作（仍在执行或最近终态）。
type DuplicateRequest struct {
	// Active 为 true 表示复用了一个仍在执行的操作；为 false 复用了最近一个终态操作。
	Active bool
	// OperationID 被复用的原操作 ID。
	OperationID string
}

func (e DuplicateRequest) Error() string {
	if e.Active {
		return "switchover operation " + e.OperationID + " is already in progress on this route"
	}
	return "switchover operation " + e.OperationID + " already ran on this route; retry or restore explicitly"
}

// ConflictError 标记试图并行改变同一线路拓扑的跨类型请求（如切换中要求恢复）。
type ConflictError struct{ Detail string }

func (e ConflictError) Error() string {
	if e.Detail == "" {
		return "another switchover operation is in progress on this route"
	}
	return e.Detail
}

// DeviceFailureError 包裹真正的设备侧失败，便于与超时、取消区分。
type DeviceFailureError struct {
	Stage string
	Err   error
}

func (e *DeviceFailureError) Error() string {
	if e.Stage == "" {
		return "device failure: " + e.Err.Error()
	}
	return e.Stage + ": " + e.Err.Error()
}

func (e *DeviceFailureError) Unwrap() error { return e.Err }

func newDeviceFailure(stage string, err error) *DeviceFailureError {
	return &DeviceFailureError{Stage: stage, Err: err}
}

type Operation struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func NewOperation(id, routeID string, kind Kind, attempt int, retryOf string, at time.Time) *Operation {
	return &Operation{snapshot: Snapshot{
		ID: id, RouteID: routeID, Kind: kind, State: StateCreated,
		Attempt: attempt, RetryOf: retryOf, UpdatedAt: at.UTC(),
	}}
}

func (o *Operation) Snapshot() Snapshot {
	o.mu.RLock()
	defer o.mu.RUnlock()
	result := o.snapshot
	result.Events = append([]Event(nil), result.Events...)
	return result
}

func (o *Operation) record(state State, stage, message string, at time.Time) Snapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshot.State = state
	o.snapshot.Events = append(o.snapshot.Events, Event{Stage: stage, Message: message, At: at.UTC()})
	o.snapshot.UpdatedAt = at.UTC()
	return o.snapshot
}

// finish 写入终态并补齐分类、可重试/可恢复标记。
func (o *Operation) finish(state State, outcome Outcome, stage, message string, at time.Time) Snapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshot.State = state
	o.snapshot.DrainOutcome = outcome
	switch outcome {
	case OutcomeDrainTimeout:
		// 排空超时但保护已建立：可重试排空切换，也可在修复后恢复主路径。
		o.snapshot.Retriable = true
		o.snapshot.Restorable = true
	case OutcomeDeviceFailure:
		o.snapshot.Retriable = true
		o.snapshot.Restorable = false
	case OutcomeCanceled:
		o.snapshot.Retriable = true
		o.snapshot.Restorable = false
	}
	if state == StateProtected && outcome == OutcomeNone {
		o.snapshot.Restorable = true
	}
	if state == StateCompleted {
		o.snapshot.Retriable = false
		o.snapshot.Restorable = false
	}
	o.snapshot.Events = append(o.snapshot.Events, Event{Stage: stage, Outcome: outcome, Message: message, At: at.UTC()})
	o.snapshot.UpdatedAt = at.UTC()
	return o.snapshot
}
