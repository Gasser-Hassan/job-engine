
CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_results (
    job_id INT REFERENCES jobs(id),
    result JSONB,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_traces (
    id SERIAL PRIMARY KEY,
    job_id INT REFERENCES jobs(id),
    step_number INT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);