package main

import (
	"net/http"
)

func uploadHandler(w http.ResponseWriter, r *http.Request){
	chunkID := r.URL.Query().Get("id")
}