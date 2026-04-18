package main
import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)
var db *sql.DB
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
func submitJob(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		fmt.Println("📩 Received request")

		var data struct {
			Type string `json:"type"`
			Data []int  `json:"data"`
		}

		err := json.NewDecoder(r.Body).Decode(&data)
		if err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		var jobID int
		err = db.QueryRow(
			"INSERT INTO jobs (type, status) VALUES ($1, 'PENDING') RETURNING id",
			data.Type,
		).Scan(&jobID)

		if err != nil {
			fmt.Println("DB ERROR:", err)
			http.Error(w, "DB error", 500)
			return
		}

		jobQueue <- Job{
			ID:   jobID,
			Type: data.Type,
			Data: data.Data,
		}

		fmt.Println("✅ Job queued:", jobID)

		json.NewEncoder(w).Encode(map[string]int{"job_id": jobID})
	}
}
// queue (channel)
var jobQueue = make(chan Job, 100)
func main() {
	connStr := "user=gasser dbname=job_engine sslmode=disable"
	var err error
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
	for i := 0; i < 3; i++ {
	go worker(i, db)
}

// setup API route
http.HandleFunc("/jobs", submitJob(db))



fmt.Println("🚀 Server running on http://localhost:8081")
log.Fatal(http.ListenAndServe(":8081", nil))
}