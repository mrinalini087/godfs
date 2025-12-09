package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

// Holds the metadata: FileName -> List of Chunk IDs
type FileMetadata struct {
	FileSize int64    `json:"fileSize"`
	Chunks   []string `json:"chunks"` // e.g., ["chunk_1", "chunk_2"]
}

var fileMap = make(map[string]FileMetadata)
var mu sync.RWMutex

func metadataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		// Client is telling us about a new file
		var meta struct {
			Name   string   `json:"name"`
			Size   int64    `json:"size"`
			Chunks []string `json:"chunks"`
		}
		json.NewDecoder(r.Body).Decode(&meta)

		mu.Lock()
		fileMap[meta.Name] = FileMetadata{FileSize: meta.Size, Chunks: meta.Chunks}
		mu.Unlock()

		fmt.Printf("Registered file: %s with %d chunks\n", meta.Name, len(meta.Chunks))

	} else if r.Method == http.MethodGet {
		// Client is asking where a file is
		name := r.URL.Query().Get("name")
		mu.RLock()
		meta, ok := fileMap[name]
		mu.RUnlock()

		if !ok {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(meta)
	}
}

func main() {
	http.HandleFunc("/metadata", metadataHandler)
	fmt.Println("NameNode running on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}