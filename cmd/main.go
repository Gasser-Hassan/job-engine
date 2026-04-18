package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)
type Job struct {
	ID   int
	Type string
	Data []int
}
func worker(id int, db *sql.DB) {
	for job := range jobQueue {
		fmt.Println("Worker", id, "processing job", job.ID)
		processJob(job, db)
	}
}
func processJob(job Job, db *sql.DB) {
	logTrace(db, job.ID, 1, "Started processing")

	cleaned := job.Data
	logTrace(db, job.ID, 2, "Cleaned data")

	sum := 0
	for _, v := range cleaned {
		sum += v
	}
	avg := sum / len(cleaned)

	logTrace(db, job.ID, 3, fmt.Sprintf("Computed average: %d", avg))

	_, _ = db.Exec(
		"INSERT INTO job_results (job_id, result) VALUES ($1, $2)",
		job.ID,
		fmt.Sprintf(`{"average": %d}`, avg),
	)

	_, _ = db.Exec(
		"UPDATE jobs SET status='COMPLETED' WHERE id=$1",
		job.ID,
	)
}
func logTrace(db *sql.DB, jobID int, step int, message string) {
	_, _ = db.Exec(
		"INSERT INTO job_traces (job_id, step_number, message) VALUES ($1, $2, $3)",
		jobID, step, message,
	)
}
// queue (channel)
var jobQueue = make(chan Job, 100)
func main() {
	connStr := "user=gasser dbname=job_engine sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("DB connection failed:", err)
	}

	fmt.Println("✅ Connected to database!")

	// 🚀 START WORKERS
	for i := 0; i < 3; i++ {
		go worker(i, db)
	}

	// 🧪 CREATE A TEST JOB (manually for now)
	var jobID int
	err = db.QueryRow(
		"INSERT INTO jobs (type, status) VALUES ($1, 'PENDING') RETURNING id",
		"TEST",
	).Scan(&jobID)

	if err != nil {
		log.Fatal(err)
	}

	// send job into queue
	jobQueue <- Job{
		ID:   jobID,
		Type: "TEST",
		Data: []int{10, 20, 30},
	}

	// 🔴 KEEP PROGRAM RUNNING (NOW THIS IS VALID)
	time.Sleep(5 * time.Second)
}