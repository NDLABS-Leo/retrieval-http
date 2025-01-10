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
	pieceCid := r.URL.Path[len("/piece/"):]
	if pieceCid == "" {
		http.Error(w, "PieceCid is required", http.StatusBadRequest)
		return
	}

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

	carFile, err := os.Open(file.CarPath)
	if err != nil {
		http.Error(w, "Failed to open CAR file", http.StatusInternalServerError)
		log.Printf("Failed to open file %s: %v", file.CarPath, err)
		return
	}
	defer carFile.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", file.CarPath))

	buffer := make([]byte, 8*1024*1024) // 8MB buffer
	for {
		n, err := carFile.Read(buffer)
		if n > 0 {
			if _, writeErr := w.Write(buffer[:n]); writeErr != nil {
				log.Printf("Failed to write to response: %v", writeErr)
				return
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("File read error: %v", err)
			http.Error(w, "Error reading file", http.StatusInternalServerError)
			return
		}
	}
}

func main() {
	http.HandleFunc("/piece/", handleRetrieval)

	// Start the HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on :%s...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
