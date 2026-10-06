package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	clock := fixedClock{now: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)}
	ids := platform.UUIDGenerator{}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	target, err := span.New("S3", "S3", 126, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	path, err := route.New("R-S3", "S3", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(0)
	activator := route.NewActivator(registry)
	receipts := landing.NewReceiptRegistry()
	permitService := repair.NewPermitService(feed.NewIsolationService(), receipts, ids, clock.Now)
	circuit, err := feed.NewCircuit("B", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	permitService.RegisterCircuit(circuit)
	repeaterAsset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	projector := repeater.NewHealthProjector(lifecycle, 85)
	projector.Register(repeaterAsset)
	repository := telemetry.NewRepository()
	switchoverManager := switchover.NewManager(drainer, activator, registry, ids, clock.Now, time.Second)
	acceptance := repair.NewAcceptanceService(switchoverManager, clock.Now)
	acceptance.Register("S3", target, path)
	services := &Services{
		Clock: clock, IDs: ids, Journal: store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl")),
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchoverManager,
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector), TelemetryRepository: repository,
		Admission: incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)),
	}
	return NewServer(services)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func TestHealthAndScanHTTPFlow(t *testing.T) {
	server := testServer(t)
	health := httptest.NewRecorder()
	server.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	body, _ := json.Marshal(ScanRequest{StationID: "east", LengthKM: 126, Chunks: 3})
	scan := httptest.NewRecorder()
	server.Handler().ServeHTTP(scan, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
	if scan.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", scan.Code, scan.Body.String())
	}
	var trace otdr.Trace
	if err := json.Unmarshal(scan.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	if trace.SpanID != "S3" || len(trace.Chunks) != 3 {
		t.Fatalf("unexpected trace: %+v", trace)
	}

	status := httptest.NewRecorder()
	server.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/system/status", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", status.Code, status.Body.String())
	}
}

func doSwitchoverRequest(t *testing.T, server *Server, method, target string) (*httptest.ResponseRecorder, switchover.Snapshot) {
	t.Helper()
	recorder := httptest.NewRecorder()
	var body io.Reader
	if method == http.MethodPost {
		body = bytes.NewReader([]byte(`{"reason":"cut"}`))
	}
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(method, target, body))
	var snap switchover.Snapshot
	if recorder.Code == http.StatusAccepted || recorder.Code == http.StatusOK {
		if err := json.Unmarshal(recorder.Body.Bytes(), &snap); err != nil {
			t.Fatalf("decode %s %s: %v body=%s", method, target, err, recorder.Body.String())
		}
	}
	return recorder, snap
}

func TestSwitchoverLifecycleHTTPFlow(t *testing.T) {
	server := testServer(t)

	// 尚无操作时查询返回 404。
	missing := httptest.NewRecorder()
	server.Handler().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/api/routes/R-S3/switchovers", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("expected 404 before any operation, got %d", missing.Code)
	}

	// 发起切换：202 + protected。
	created, operation := doSwitchoverRequest(t, server, http.MethodPost, "/api/routes/R-S3/switchovers")
	if created.Code != http.StatusAccepted {
		t.Fatalf("switchover status=%d body=%s", created.Code, created.Body.String())
	}
	if operation.State != switchover.StateProtected || operation.Kind != switchover.KindFailover {
		t.Fatalf("unexpected operation: %+v", operation)
	}

	// 重复切换复用原操作：200 + 同一 ID。
	duplicate, dupSnap := doSwitchoverRequest(t, server, http.MethodPost, "/api/routes/R-S3/switchovers")
	if duplicate.Code != http.StatusOK || dupSnap.ID != operation.ID {
		t.Fatalf("duplicate must return 200 and same id, got %d id=%s body=%s", duplicate.Code, dupSnap.ID, duplicate.Body.String())
	}

	// 按路由查询最近操作、按 ID 查询单操作。
	latest, latestSnap := doSwitchoverRequest(t, server, http.MethodGet, "/api/routes/R-S3/switchovers")
	if latest.Code != http.StatusOK || latestSnap.ID != operation.ID {
		t.Fatalf("latest lookup failed: %d %s", latest.Code, latest.Body.String())
	}
	byID, idSnap := doSwitchoverRequest(t, server, http.MethodGet, "/api/switchovers/"+operation.ID)
	if byID.Code != http.StatusOK || idSnap.ID != operation.ID {
		t.Fatalf("operation lookup failed: %d %s", byID.Code, byID.Body.String())
	}

	// 未切换前对成功操作发起 retry 必须冲突（不可重试）。
	retryRejected := httptest.NewRecorder()
	server.Handler().ServeHTTP(retryRejected, httptest.NewRequest(http.MethodPost, "/api/routes/R-S3/switchovers/"+operation.ID+"/retry", bytes.NewReader([]byte(`{}`))))
	if retryRejected.Code != http.StatusConflict {
		t.Fatalf("retry on non-retriable op must be 409, got %d body=%s", retryRejected.Code, retryRejected.Body.String())
	}

	// 受控恢复主路径：202 + completed。
	restored, restoreSnap := doSwitchoverRequest(t, server, http.MethodPost, "/api/routes/R-S3/restore")
	if restored.Code != http.StatusAccepted || restoreSnap.State != switchover.StateCompleted {
		t.Fatalf("restore status=%d snap=%+v body=%s", restored.Code, restoreSnap, restored.Body.String())
	}

	// 再次恢复复用已完成记录。
	again, againSnap := doSwitchoverRequest(t, server, http.MethodPost, "/api/routes/R-S3/restore")
	if again.Code != http.StatusOK || againSnap.ID != restoreSnap.ID {
		t.Fatalf("duplicate restore must return 200 same id, got %d body=%s", again.Code, again.Body.String())
	}

	// 已在主路径，恢复完成后路由状态为主路径。
	routeRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(routeRecorder, httptest.NewRequest(http.MethodGet, "/api/routes/R-S3", nil))
	var routeSnapshot struct {
		ActivePath topology.Path `json:"active_path"`
	}
	if err := json.Unmarshal(routeRecorder.Body.Bytes(), &routeSnapshot); err != nil {
		t.Fatal(err)
	}
	if routeSnapshot.ActivePath != topology.PathPrimary {
		t.Fatalf("route should be back on primary, got %q", routeSnapshot.ActivePath)
	}
}
