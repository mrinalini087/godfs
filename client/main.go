package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// We will split data into 3 chunks for this demo
var dataNodes = []string{
	"http://localhost:9001",
	"http://localhost:9002",
	"http://localhost:9003",
}

func main() {
	// 1. The "File" we want to upload (Imagine this is a large video file)
	fileName := "my_big_data.txt"
	content := "This is a distributed file system. It splits data into blocks. This is block three."
	
	// We will split this string into 3 parts
	partSize := len(content) / 3
	var chunkIDs []string

	fmt.Printf("Uploading %s (%d bytes)...\n", fileName, len(content))

	// 2. Loop through 3 DataNodes and upload a piece to each
	for i := 0; i < 3; i++ {
		// Calculate the slice of the string
		start := i * partSize
		end := start + partSize
		if i == 2 { end = len(content) } // Ensure last chunk gets the rest

		chunkData := content[start:end]
		chunkID := fileName + "_chunk_" + strconv.Itoa(i)
		chunkIDs = append(chunkIDs, chunkID)

		// Send to DataNode
		nodeURL := dataNodes[i]
		// We pass the port in the query so the node knows which folder to use
		port := "900" + strconv.Itoa(i+1) 
		
		resp, err := http.Post(
			fmt.Sprintf("%s/upload?id=%s&port=%s", nodeURL, chunkID, port),
			"text/plain",
			bytes.NewBufferString(chunkData),
		)
		if err != nil {
			fmt.Println("Failed to upload to node:", err)
			return
		}
		resp.Body.Close()
		fmt.Printf(" - Uploaded %s to %s\n", chunkID, nodeURL)
	}

	// 3. Tell the NameNode about the file
	metadata := map[string]interface{}{
		"name":   fileName,
		"size":   len(content),
		"chunks": chunkIDs,
	}
	metaBytes, _ := json.Marshal(metadata)
	http.Post("http://localhost:8000/metadata", "application/json", bytes.NewBuffer(metaBytes))
	
	fmt.Println("Success! File metadata sent to NameNode.")
}