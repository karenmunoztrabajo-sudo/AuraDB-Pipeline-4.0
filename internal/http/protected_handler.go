package http

import (
	"encoding/json"
	"net/http"
)

func ProtectedWhoAmI(w http.ResponseWriter, r *http.Request) {
	ctxIPC, ok := GetIPC(r)
	if !ok {
		http.Error(w, "ipc no encontrado", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ctxIPC)
}