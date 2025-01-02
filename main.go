package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type SealingFileModel struct {
	Id       int64  `gorm:"primarykey" json:"id"`
	CarPath  string `gorm:"column:car_path" json:"car_path"`
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

func handleRetrieval(w http.ResponseWriter, r *http.Request) {
	// Extract pieceCid from the URL
	pieceCid := r.URL.Path[len("/retrieval/"):]
	if pieceCid == "" {
		http.Error(w, "PieceCid is required", http.StatusBadRequest)
		return
	}

	// Query the database for the pieceCid
	var file SealingFileModel
	if err := db.First(&file, "piece_cid = ?", pieceCid).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			http.Error(w, "No data found", http.StatusNotFound)
		} else {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			log.Printf("Database error: %v", err)
		}
		return
	}

	// Open the CAR file
	carFile, err := os.Open(file.CarPath)
	if err != nil {
		http.Error(w, "Failed to open CAR file", http.StatusInternalServerError)
		log.Printf("Failed to open file %s: %v", file.CarPath, err)
		return
	}
	defer carFile.Close()

	// Stream the CAR file content
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", file.CarPath))
	if _, err := io.Copy(w, carFile); err != nil {
		log.Printf("Failed to stream file %s: %v", file.CarPath, err)
	}
}

func main() {
	http.HandleFunc("/retrieval/", handleRetrieval)

	// Start the HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on :%s...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
