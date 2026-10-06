package repair

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/switchover"
)

type Acceptance struct {
	ID        string           `json:"id"`
	SpanID    string           `json:"span_id"`
	Proofs    otdr.ProofResult `json:"proofs"`
	State     span.State       `json:"state"`
	Restored  bool             `json:"restored"`
	UpdatedAt time.Time        `json:"updated_at"`
}

type primaryRestorer interface {
	Restore(ctx context.Context, target *route.Route) (switchover.Snapshot, error)
}

type AcceptanceService struct {
	mu          sync.Mutex
	proofSets   map[string]*otdr.ProofSet
	acceptances map[string]Acceptance
	spans       map[string]*span.Span
	routes      map[string]*route.Route
	restorer    primaryRestorer
	now         func() time.Time
}

func NewAcceptanceService(restorer primaryRestorer, now func() time.Time) *AcceptanceService {
	return &AcceptanceService{
		proofSets:   make(map[string]*otdr.ProofSet),
		acceptances: make(map[string]Acceptance),
		spans:       make(map[string]*span.Span),
		routes:      make(map[string]*route.Route),
		restorer:    restorer,
		now:         now,
	}
}

func (s *AcceptanceService) Register(spanID string, target *span.Span, route *route.Route) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans[spanID] = target
	s.routes[spanID] = route
}

func (s *AcceptanceService) AddProof(ctx context.Context, acceptanceID, spanID string, proof otdr.Proof) (Acceptance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.proofSets[acceptanceID]
	if set == nil {
		set = otdr.NewProofSet()
		s.proofSets[acceptanceID] = set
	}
	result, err := set.Add(proof)
	if err != nil {
		return Acceptance{}, err
	}
	target := s.spans[spanID]
	path := s.routes[spanID]
	if target == nil || path == nil {
		return Acceptance{}, fmt.Errorf("acceptance span %s is not registered", spanID)
	}
	acceptance := Acceptance{ID: acceptanceID, SpanID: spanID, Proofs: result, State: target.Snapshot().State, UpdatedAt: s.now().UTC()}
	if !result.Complete {
		s.acceptances[acceptanceID] = acceptance
		return acceptance, nil
	}
	if !result.Passed {
		if target.Snapshot().State == span.StateProving {
			_, _ = target.RejectProof(s.now())
		}
		acceptance.State = target.Snapshot().State
		s.acceptances[acceptanceID] = acceptance
		return acceptance, fmt.Errorf("splice optical proof failed")
	}
	if target.Snapshot().State != span.StateProving {
		return Acceptance{}, fmt.Errorf("span %s is not in proving state", spanID)
	}
	if _, err := s.restorer.Restore(ctx, path); err != nil && !errors.Is(err, switchover.ErrAlreadyPrimary) {
		return Acceptance{}, err
	}
	if _, err := target.Recover(s.now()); err != nil {
		return Acceptance{}, err
	}
	acceptance.State = target.Snapshot().State
	acceptance.Restored = true
	s.acceptances[acceptanceID] = acceptance
	return acceptance, nil
}

func (s *AcceptanceService) Get(id string) (Acceptance, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, exists := s.acceptances[id]
	return value, exists
}
