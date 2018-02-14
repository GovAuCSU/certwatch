package main

import (
	"errors"
	"log"
	"time"

	que "github.com/bgentry/que-go"
	"github.com/jackc/pgx"
)

var (
	ErrTryAgainPlease = errors.New("try again, now we have metadata")
)

func singletonCron(qc *que.Client, logger *log.Logger, jobClass, args string, job *que.Job, f func(tx *pgx.Tx) error) error {
	tx, err := job.Conn().Begin()
	if err != nil {
		return err
	}
	// Per docs it is safe call rollback() as a noop on an already closed transaction
	defer tx.Rollback()

	logger.Println("starting", job.ID)
	defer logger.Println("stop", job.ID)

	key := jobClass + args

	var lastCompleted time.Time
	var nextScheduled time.Time
	err = tx.QueryRow("SELECT last_completed, next_scheduled FROM cron_metadata WHERE id = $1 FOR UPDATE", key).Scan(&lastCompleted, &nextScheduled)
	if err != nil {
		if err == pgx.ErrNoRows {
			_, err = tx.Exec("INSERT INTO cron_metadata (id) VALUES ($1)", key)
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

	logger.Println("got mutex", job.ID)
	if time.Now().Before(nextScheduled) {
		var futureJobs int
		err = tx.QueryRow("SELECT count(*) FROM que_jobs WHERE job_class = $1 AND args = $2 AND run_at >= $3", jobClass, args, nextScheduled).Scan(&futureJobs)
		if err != nil {
			return err
		}

		if futureJobs > 0 {
			logger.Println("Enough future jobs already scheduled.", job.ID)
			return nil
		}

		logger.Println("No future jobs found, scheduling one to match end time", job.ID)
		err = qc.EnqueueInTx(&que.Job{
			Type:  jobClass,
			Args:  []byte(args),
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
		Type:  jobClass,
		Args:  []byte(args),
		RunAt: next,
	}, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}
