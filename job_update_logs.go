package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/bgentry/que-go"
	"github.com/jackc/pgx"
)

const (
	StateActive = 0
	StateIgnore = 1
)

type MonitoredLog struct {
	URL       string `sql:",pk"`
	Processed int64  `sql:",notnull"`
	State     int    `sql:",notnull"`
}

type CronMetadata struct {
	ID            string    `sql:",pk"`
	LastCompleted time.Time `sql:",notnull"`
	NextScheduled time.Time `sql:",notnull"`
}

type LogUpdater struct {
	QC     *que.Client
	URL    string
	Logger *log.Logger
}

const (
	KeyUpdateLogs = "update_logs"
)

func enqueueCron(qc *que.Client, key string, when time.Time) error {
	return qc.Enqueue(&que.Job{
		Args:  []byte("{}"),
		Type:  key,
		RunAt: when,
	})
}

func singletonCron(qc *que.Client, logger *log.Logger, key string, job *que.Job, f func(tx *pgx.Tx) error) error {
	tx, err := job.Conn().Begin()
	if err != nil {
		return err
	}

	logger.Println("starting", job.ID)
	defer logger.Println("stop", job.ID)

	row := tx.QueryRow("SELECT last_completed, next_scheduled FROM cron_metadata WHERE id = $1 FOR UPDATE", key)
	var lastCompleted time.Time
	var nextScheduled time.Time
	err = row.Scan(&lastCompleted, &nextScheduled)
	if err != nil {
		tx.Rollback()
		return err
	}

	logger.Println("got mutex", job.ID)
	if time.Now().Before(nextScheduled) {
		var futureJobs int
		err = tx.QueryRow("SELECT count(*) FROM que_jobs WHERE job_class = $1 AND run_at >= $2", key, nextScheduled).Scan(&futureJobs)
		if err != nil {
			panic(err)
			return err
		}

		if futureJobs > 0 {
			logger.Println("Enough future jobs already scheduled.", job.ID)
			tx.Rollback()
			return nil
		}

		logger.Println("No future jobs found, scheduling one to match end time", job.ID)
		err = qc.EnqueueInTx(&que.Job{
			Args:  []byte("{}"),
			Type:  key,
			RunAt: nextScheduled,
		}, tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	err = f(tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	n := time.Now()
	next := n.Add(time.Hour * 24)

	_, err = tx.Exec("UPDATE cron_metadata SET last_completed = $1, next_scheduled = $2 WHERE id = $3", n, next, key)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = qc.EnqueueInTx(&que.Job{
		Args:  []byte("{}"),
		Type:  key,
		RunAt: next,
	}, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (j *LogUpdater) Run(job *que.Job) error {
	return singletonCron(j.QC, j.Logger, KeyUpdateLogs, job, func(tx *pgx.Tx) error {
		resp, err := http.Get(j.URL)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.New("bad status code")
		}

		var logData struct {
			Logs []struct {
				URL            string `json:"url"`
				DisqualifiedAt int64  `json:"disqualified_at"`
				FinalSTH       *struct {
					TreeSize int64 `json:"tree_size"`
				} `json:"final_sth"`
			} `json:"logs"`
		}

		err = json.NewDecoder(resp.Body).Decode(&logData)
		if err != nil {
			return err
		}

		for _, l := range logData.Logs {
			dirty := false

			var dbProcessed int64
			var dbState int

			err := tx.QueryRow("SELECT processed, state FROM monitored_logs WHERE url = $1", l.URL).Scan(&dbProcessed, &dbState)
			if err != nil {
				if err == pgx.ErrNoRows {
					dirty = true
				} else {
					return err
				}
			}

			if (l.FinalSTH != nil || l.DisqualifiedAt != 0) && dbState == StateActive {
				dirty = true
				dbState = StateIgnore
			}

			if dirty {
				_, err = tx.Exec("INSERT INTO monitored_logs (url, state, processed) VALUES ($1, $2, $3) ON CONFLICT (url) DO UPDATE SET state = $4", l.URL, dbState, dbProcessed, dbState)
				if err != nil {
					j.Logger.Println("ERROR updating log for: ", l.URL, err)
					return err
				}
				j.Logger.Println("Updated metadata for ", l.URL)
			} else {
				j.Logger.Println("No changes in metadata for ", l.URL)
			}
		}

		return nil
	})
}
