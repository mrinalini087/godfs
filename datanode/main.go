package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Get the Chunk ID from the URL (e.g., /upload?id=chunk1)
	chunkID := r.URL.Query().Get("id")
	if chunkID == "" {
		http.Error(w, "Missing chunk ID", http.StatusBadRequest)
		return
	}

	// 2. Create a specific folder for this node's storage
	// We use the port number to create separate folders for separate nodes on one machine
	port := r.URL.Query().Get("port") 
	storagePath := fmt.Sprintf("storage_%s", port)
	os.MkdirAll(storagePath, os.ModePerm) // Create folder if not exists

	// 3. Create the file on disk
	dst, err := os.Create(filepath.Join(storagePath, chunkID))
	if err != nil {
		http.Error(w, "Could not create file", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// 4. Copy the data from the network request to the file
	if _, err := io.Copy(dst, r.Body); err != nil {
		http.Error(w, "Failed to save data", http.StatusInternalServerError)
		return
	}

	fmt.Printf("Stored chunk %s in %s\n", chunkID, storagePath)
	w.WriteHeader(http.StatusOK)
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	chunkID := r.URL.Query().Get("id")
	port := r.URL.Query().Get("port")
	storagePath := fmt.Sprintf("storage_%s", port)

	// Read the file from disk and send it back
	data, err := os.ReadFile(filepath.Join(storagePath, chunkID))
	if err != nil {
		http.Error(w, "Chunk not found", http.StatusNotFound)
		return
	}

	w.Write(data)
}

func main() {
	portPtr := flag.String("port", "9000", "port number")
	flag.Parse()
	port := ":" + *portPtr

	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/download", downloadHandler)

	fmt.Printf("DataNode running on port %s\n", *portPtr)
	log.Fatal(http.ListenAndServe(port, nil))
}