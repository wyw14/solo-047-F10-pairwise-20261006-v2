package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/switchover"
)

func (s *Server) startSwitchover(writer http.ResponseWriter, request *http.Request) {
	routeID := chi.URLParam(request, "id")
	target, err := s.services.Route(routeID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input SwitchoverRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	var result switchover.Snapshot
	switch input.Action {
	case "", "failover":
		result, err = s.services.Switchovers.Failover(request.Context(), target)
	case "retry":
		result, err = s.services.Switchovers.Retry(request.Context(), target)
	case "restore-primary":
		result, err = s.services.Switchovers.RestorePrimary(request.Context(), target)
	default:
		writeJSON(writer, http.StatusBadRequest, ErrorResponse{Error: "unsupported switchover action"})
		return
	}
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("switchover", result.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}

func (s *Server) getRoute(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}
