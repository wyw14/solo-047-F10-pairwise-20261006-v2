package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/switchover"
)

func (s *Server) startSwitchover(writer http.ResponseWriter, request *http.Request) {
	target, ok := s.routeTarget(writer, request)
	if !ok {
		return
	}
	var input SwitchoverRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	_ = input
	result, err := s.services.Switchovers.Failover(request.Context(), target)
	if err != nil {
		var duplicate switchover.DuplicateRequest
		if errors.As(err, &duplicate) {
			s.respondOperation(writer, http.StatusOK, result)
			return
		}
		writeError(writer, switchoverErrorStatus(err), err)
		return
	}
	s.respondOperation(writer, http.StatusAccepted, result)
}

func (s *Server) retrySwitchover(writer http.ResponseWriter, request *http.Request) {
	target, ok := s.routeTarget(writer, request)
	if !ok {
		return
	}
	operationID := chi.URLParam(request, "operationID")
	result, err := s.services.Switchovers.Retry(request.Context(), operationID, target)
	if err != nil {
		var duplicate switchover.DuplicateRequest
		if errors.As(err, &duplicate) {
			s.respondOperation(writer, http.StatusOK, result)
			return
		}
		writeError(writer, switchoverErrorStatus(err), err)
		return
	}
	s.respondOperation(writer, http.StatusAccepted, result)
}

func (s *Server) restorePrimary(writer http.ResponseWriter, request *http.Request) {
	target, ok := s.routeTarget(writer, request)
	if !ok {
		return
	}
	result, err := s.services.Switchovers.Restore(request.Context(), target)
	if err != nil {
		var duplicate switchover.DuplicateRequest
		if errors.As(err, &duplicate) {
			s.respondOperation(writer, http.StatusOK, result)
			return
		}
		writeError(writer, switchoverErrorStatus(err), err)
		return
	}
	s.respondOperation(writer, http.StatusAccepted, result)
}

func (s *Server) latestSwitchover(writer http.ResponseWriter, request *http.Request) {
	routeID := chi.URLParam(request, "id")
	result, exists := s.services.Switchovers.Latest(routeID)
	if !exists {
		writeError(writer, http.StatusNotFound, switchover.ConflictError{Detail: "no switchover operation for route " + routeID})
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (s *Server) getSwitchover(writer http.ResponseWriter, request *http.Request) {
	operationID := chi.URLParam(request, "operationID")
	result, exists := s.services.Switchovers.Operation(operationID)
	if !exists {
		writeError(writer, http.StatusNotFound, switchover.ConflictError{Detail: "switchover operation " + operationID + " not found"})
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (s *Server) getRoute(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}

func (s *Server) routeTarget(writer http.ResponseWriter, request *http.Request) (*route.Route, bool) {
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return nil, false
	}
	return target, true
}

func (s *Server) respondOperation(writer http.ResponseWriter, status int, result switchover.Snapshot) {
	if err := s.services.Persist("switchover", result.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, status, result)
}

func switchoverErrorStatus(err error) int {
	var conflict switchover.ConflictError
	if errors.As(err, &conflict) || errors.Is(err, switchover.ErrAlreadyPrimary) {
		return http.StatusConflict
	}
	var device *switchover.DeviceFailureError
	if errors.As(err, &device) {
		return http.StatusBadGateway
	}
	if errors.Is(err, context.Canceled) {
		return http.StatusConflict
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	return http.StatusInternalServerError
}
