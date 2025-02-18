package main

import (
	"errors"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type SealingFileModel struct {
	Id       int64  `gorm:"primarykey" json:"id"`
	CarPath  string `gorm:"column:car_path" json:"car_path"`
	RootCid  string `gorm:"column:root_cid" json:"root_cid"`
	PieceCid string `gorm:"column:piece_cid" json:"piece_cid"`
}

func (SealingFileModel) TableName() string {
	return "bh_sealingfile_info"
}

var db *gorm.DB

func init() {
	// Load MySQL configuration from environment variables
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		log.Fatal("Environment variable MYSQL_DSN is required")
	}

	// Connect to MySQL database
	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
}

// handleRetrievalRoot handles content retrieval requests for the root block
func handleRetrievalRoot(w http.ResponseWriter, r *http.Request) {
	// Extract rootCid from the URL path
	rootCid := r.URL.Path[len("/root/"):]
	if rootCid == "" {
		http.Error(w, "RootCid is required", http.StatusBadRequest)
		log.Printf("[ERROR] RootCid is missing in URL path: %s", r.URL.Path)
		return
	}

	// Log received request
	log.Printf("[INFO] Received request to retrieve file for CID: %s", rootCid)

	// Query the database for the CAR file based on the rootCid
	var file SealingFileModel
	if err := db.First(&file, "root_cid = ?", rootCid).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			http.Error(w, "No data found", http.StatusNotFound)
			log.Printf("[ERROR] No data found for CID: %s", rootCid)
		} else {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			log.Printf("[ERROR] Database error: %v", err)
		}
		return
	}

	// Log file information
	log.Printf("[INFO] Found CAR file for CID: %s, Path: %s", rootCid, file.CarPath)

	// Open the CAR file from the path stored in the database
	carFile, err := os.Open(file.CarPath)
	if err != nil {
		http.Error(w, "Failed to open CAR file", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to open CAR file %s: %v", file.CarPath, err)
		return
	}
	defer carFile.Close()

	// Log the start of CAR file reading
	log.Printf("[INFO] Opening CAR file: %s", file.CarPath)

	// Define the size to read (4MB)
	readSize := 4 * 1024 * 1024
	buffer := make([]byte, readSize)

	// Read the first 4MB from the CAR file
	bytesRead, err := carFile.Read(buffer)
	if err != nil && err != io.EOF {
		http.Error(w, "Failed to read CAR file", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to read from CAR file %s: %v", file.CarPath, err)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", bytesRead))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", bytesRead-1, bytesRead))

	// Log the headers
	log.Printf("[INFO] Sending response with Content-Length: %d, Content-Range: bytes 0-%d/%d", bytesRead, bytesRead-1, bytesRead)

	// Write the data to the response body
	_, err = w.Write(buffer[:bytesRead])
	if err != nil {
		http.Error(w, "Failed to write block data to response", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to write block data to response: %v", err)
		return
	}

	// Log the successful completion of the request
	log.Printf("[INFO] Successfully returned first 4MB of CAR file for CID: %s", rootCid)
}

// handleRetrievalPiece handles content retrieval requests for the piece block
// handleRetrievalPiece handles content retrieval requests for the piece block
func handleRetrievalPiece(w http.ResponseWriter, r *http.Request) {
	// Extract pieceCid from the URL path
	pieceCid := r.URL.Path[len("/piece/"):]
	if pieceCid == "" {
		http.Error(w, "PieceCid is required", http.StatusBadRequest)
		log.Printf("[ERROR] PieceCid is missing in URL path: %s", r.URL.Path)
		return
	}

	// Log received request
	log.Printf("[INFO] Received request to retrieve piece for CID: %s", pieceCid)

	// Query the database for the CAR file based on the pieceCid
	var file SealingFileModel
	if err := db.First(&file, "piece_cid = ?", pieceCid).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			http.Error(w, "No data found", http.StatusNotFound)
			log.Printf("[ERROR] No data found for PieceCid: %s", pieceCid)
		} else {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			log.Printf("[ERROR] Database error: %v", err)
		}
		return
	}

	// Log file information
	log.Printf("[INFO] Found CAR file for PieceCid: %s, Path: %s", pieceCid, file.CarPath)

	// Open the CAR file from the path stored in the database
	carFile, err := os.Open(file.CarPath)
	if err != nil {
		http.Error(w, "Failed to open CAR file", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to open CAR file %s: %v", file.CarPath, err)
		return
	}
	defer carFile.Close()

	// Log the start of CAR file reading
	log.Printf("[INFO] Opening CAR file: %s", file.CarPath)

	// Get file size
	fileStat, err := carFile.Stat()
	if err != nil {
		http.Error(w, "Failed to get file size", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to stat CAR file: %v", err)
		return
	}
	fileSize := fileStat.Size()

	// Process Range request
	rangeHeader := r.Header.Get("Range")
	var start, end int64

	if rangeHeader != "" {
		// Parse the range header (e.g., "bytes=0-999")
		if !strings.HasPrefix(rangeHeader, "bytes=") {
			http.Error(w, "Invalid Range header", http.StatusBadRequest)
			return
		}

		rangeStr := rangeHeader[len("bytes="):]
		rangeParts := strings.Split(rangeStr, "-")
		if len(rangeParts) != 2 {
			http.Error(w, "Invalid Range format", http.StatusBadRequest)
			return
		}

		start, _ = strconv.ParseInt(rangeParts[0], 10, 64)
		if rangeParts[1] != "" {
			end, _ = strconv.ParseInt(rangeParts[1], 10, 64)
		} else {
			end = fileSize - 1
		}

		if end > fileSize-1 {
			end = fileSize - 1
		}
		if start > end {
			http.Error(w, "Invalid Range", http.StatusBadRequest)
			return
		}

		// Set appropriate headers for partial content
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		// If no Range header, send the full file
		start = 0
		end = fileSize - 1
		w.Header().Set("Content-Length", fmt.Sprintf("%d", fileSize))
	}

	// Seek to the correct start position in the CAR file
	_, err = carFile.Seek(start, 0)
	if err != nil {
		http.Error(w, "Failed to seek to position in CAR file", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to seek: %v", err)
		return
	}

	// Stream the file content to the response
	buf := make([]byte, 8192) // 8KB buffer
	totalBytesWritten := int64(0)

	for totalBytesWritten < (end - start + 1) {
		bytesToRead := int64(len(buf))
		if totalBytesWritten+bytesToRead > (end - start + 1) {
			bytesToRead = (end - start + 1) - totalBytesWritten
		}

		// Read the data from the CAR file
		n, err := carFile.Read(buf[:bytesToRead])
		if err != nil && err.Error() != "EOF" {
			http.Error(w, "Error reading from CAR file", http.StatusInternalServerError)
			log.Printf("[ERROR] Failed to read from CAR file: %v", err)
			return
		}

		// Write the chunk to the response
		_, err = w.Write(buf[:n])
		if err != nil {
			http.Error(w, "Failed to write data to response", http.StatusInternalServerError)
			log.Printf("[ERROR] Failed to write data: %v", err)
			return
		}

		// Update the number of bytes written
		totalBytesWritten += int64(n)
	}

	// Log the successful completion of the request
	log.Printf("[INFO] Successfully returned piece for PieceCid: %s", pieceCid)
}

func main() {
	// Handle routes
	http.HandleFunc("/root/", handleRetrievalRoot)
	http.HandleFunc("/piece/", handleRetrievalPiece)

	// Start the HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default port
	}

	log.Printf("[INFO] Starting HTTP server on port %s...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
