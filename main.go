package main

import (
	"errors"
	"fmt"
	"github.com/ipfs/go-car"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"net/http"
	"os"
)

type SealingFileModel struct {
	Id      int64  `gorm:"primarykey" json:"id"`
	CarPath string `gorm:"column:car_path" json:"car_path"`
	RootCid string `gorm:"column:root_cid" json:"root_cid"`
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

// handleRetrieval handles content retrieval requests
func handleRetrieval(w http.ResponseWriter, r *http.Request) {
	// Extract rootCid from the URL path
	rootCid := r.URL.Path[len("/piece/"):]
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

	// Use go-car to read the CAR file
	carReader, err := car.NewCarReader(carFile)
	if err != nil {
		http.Error(w, "Failed to read CAR file", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to create CAR reader: %v", err)
		return
	}

	// Read the first block from the CAR file
	block, err := carReader.Next()
	if err != nil {
		http.Error(w, "Failed to retrieve block from CAR file", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to read first block: %v", err)
		return
	}

	// Log the block's CID and size
	log.Printf("[INFO] Successfully retrieved first block. CID: %s, Size: %d bytes", block.Cid(), len(block.RawData()))

	// Set response headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(block.RawData())))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(block.RawData())-1, len(block.RawData())))

	// Log headers sent to client
	log.Printf("[INFO] Sending response with Content-Length: %d, Content-Range: bytes 0-%d/%d", len(block.RawData()), len(block.RawData())-1, len(block.RawData()))

	// Write the block's data to the response body
	_, err = w.Write(block.RawData())
	if err != nil {
		http.Error(w, "Failed to write block data to response", http.StatusInternalServerError)
		log.Printf("[ERROR] Failed to write block data to response: %v", err)
		return
	}

	// Log the successful completion of the request
	log.Printf("[INFO] Successfully returned first block for CID: %s", rootCid)
}

func main() {
	http.HandleFunc("/piece/", handleRetrieval)

	// Start the HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default port
	}

	log.Printf("[INFO] Starting HTTP server on port %s...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
