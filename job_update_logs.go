package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/bgentry/que-go"
	"github.com/go-pg/pg"
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
	DB     *pg.DB
	URL    string
	Logger *log.Logger
}

const (
	KeyUpdateLogs = "update_logs"
)

func singletonCron(db *pg.DB, logger *log.Logger, key string, jobID int64, f func(tx *pg.Tx) error) error {
	return db.RunInTransaction(func(tx *pg.Tx) error {
		logger.Println("starting", jobID)
		defer logger.Println("stop", jobID)

		cm := &CronMetadata{ID: key}
		_, err := tx.Model(cm).For("UPDATE").SelectOrInsert()
		if err != nil {
			return err
		}

		logger.Println("got mutex", jobID)
		if time.Now().Before(cm.NextScheduled) {
			logger.Println("too early, come back later", jobID)
			return nil
		}

		err = f(tx)
		if err != nil {
			return err
		}

		cm.LastCompleted = time.Now()
		cm.NextScheduled = time.Now().Add(time.Hour * 24)

		return tx.Update(cm)
	})
}

func (j *LogUpdater) Run(job *que.Job) error {
	return singletonCron(j.DB, j.Logger, KeyUpdateLogs, job.ID, func(tx *pg.Tx) error {
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
			ml := &MonitoredLog{
				URL: l.URL,
			}
			dirty := false
			exists := false
			err := tx.Select(ml)
			if err == nil {
				exists = true
			} else {
				if isErrNotFound(err) {
					// that's fine, we will create and save.
					dirty = true
				} else {
					j.Logger.Println("ERROR reading monitored log, skipping: ", err)
					continue
				}
			}

			if (l.FinalSTH != nil || l.DisqualifiedAt != 0) && ml.State == StateActive {
				ml.State = StateIgnore
				dirty = true
			}

			if dirty {
				if exists {
					err = tx.Update(ml)
				} else {
					err = tx.Insert(ml)
				}
				if err != nil {
					j.Logger.Println("ERROR updating log for: ", ml.URL)
					continue
				}

				j.Logger.Println("Updated metadata for ", ml.URL)
			} else {
				j.Logger.Println("No changes in metadata for ", ml.URL)
			}
		}

		return nil
	})
}
