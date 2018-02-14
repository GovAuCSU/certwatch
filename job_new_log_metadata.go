package main

import (
	"encoding/json"
	"log"

	que "github.com/bgentry/que-go"
	"github.com/jackc/pgx"
)

type NewLogMetadata struct {
	QC     *que.Client
	Logger *log.Logger
}

func (j *NewLogMetadata) Run(job *que.Job) error {
	j.Logger.Println("start")
	defer j.Logger.Println("stop")

	var l CTLog
	err := json.Unmarshal(job.Args, &l)
	if err != nil {
		return err
	}

	tx, err := job.Conn().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var dbState int
	err = tx.QueryRow("SELECT state FROM monitored_logs WHERE url = $1 FOR UPDATE", l.URL).Scan(&dbState)
	if err != nil {
		if err == pgx.ErrNoRows {
			_, err = tx.Exec("INSERT INTO monitored_logs (url) VALUES ($1)", l.URL)
			if err != nil {
				return err
			}
			err = tx.Commit()
			if err != nil {
				return err
			}
			return ErrTryAgainPlease
		}
		return err
	}

	if (l.FinalSTH != nil || l.DisqualifiedAt != 0) && dbState == StateActive {
		_, err = tx.Exec("UPDATE monitored_logs SET state = $1 WHERE url = $2", StateIgnore, l.URL)
		if err != nil {
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}
