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
	KeyUpdateLogs     = "cron_update_logs"
	KeyNewLogMetadata = "new_log_metadata"
)

type CTLog struct {
	URL            string `json:"url"`
	DisqualifiedAt int64  `json:"disqualified_at"`
	FinalSTH       *struct {
		TreeSize int64 `json:"tree_size"`
	} `json:"final_sth"`
}

func (j *LogUpdater) Run(job *que.Job) error {
	return singletonCron(j.QC, j.Logger, KeyUpdateLogs, "{}", job, func(tx *pgx.Tx) error {
		resp, err := http.Get(j.URL)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.New("bad status code")
		}

		var logData struct {
			Logs []*CTLog `json:"logs"`
		}

		err = json.NewDecoder(resp.Body).Decode(&logData)
		if err != nil {
			return err
		}

		for _, l := range logData.Logs {
			bb, err := json.Marshal(l)
			if err != nil {
				return err
			}
			err = j.QC.EnqueueInTx(&que.Job{
				Type: KeyNewLogMetadata,
				Args: bb,
			}, tx)
			if err != nil {
				return err
			}

			/*var dbState int

			err := tx.QueryRow("SELECT state FROM monitored_logs WHERE url = $1 FOR UPDATE", l.URL).Scan(&dbState)
			if err != nil {
				if err == pgx.ErrNoRows {
					_, err = tx.Exec("INSERT INTO monitored_logs (url) VALUES ($1)", l.URL)
					if err != nil {
						return err
					}
				} else {
					return err
				}
			}

			if (l.FinalSTH != nil || l.DisqualifiedAt != 0) && dbState == StateActive {
				_, err = tx.Exec("UPDATE monitored_logs SET state = $1 WHERE url = $2", StateIgnore, l.URL)
				if err != nil {
					return err
				}
			}*/
		}

		return nil
	})
}
