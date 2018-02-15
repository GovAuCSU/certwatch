package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	que "github.com/bgentry/que-go"
	"github.com/jackc/pgx"
)

type CheckSTHConf struct {
	URL string
}

const (
	KeyCheckSTH = "cron_check_sth"
)

func CheckLogSTH(qc *que.Client, logger *log.Logger, job *que.Job, tx *pgx.Tx) error {
	var md CheckSTHConf
	err := json.Unmarshal(job.Args, &md)
	if err != nil {
		return err
	}

	// Get a lock, though we should already have one via other means
	var state int
	var processed int64

	// ensure state is active, else return error
	err = tx.QueryRow("SELECT state, processed FROM monitored_logs WHERE url = $1 FOR UPDATE", md.URL).Scan(&state, &processed)
	if err != nil {
		return err
	}

	// Don't reschedule us as a cron please.
	if state != StateActive {
		return ErrDoNotReschedule
	}

	// TODO - bit icky - a poisoned CT logs list could maybe lead to us make requests
	// where we'd prefer not to. But since it's unauthenticated, hosted by Google,
	// and the results aren't show, I think it's safe enough
	resp, err := http.Get(fmt.Sprintf("https://%sct/v1/get-sth", md.URL))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("bad status code")
	}

	var sth struct {
		TreeSize int64 `json:"tree_size"`
	}

	err = json.NewDecoder(resp.Body).Decode(&sth)
	if err != nil {
		return err
	}

	if sth.TreeSize > processed {
		// We have work to do!
		bb, err := json.Marshal(&GetEntriesConf{
			URL:   md.URL,
			Start: processed,
			End:   sth.TreeSize,
		})
		if err != nil {
			return err
		}
		err = qc.EnqueueInTx(&que.Job{
			Type: KeyGetEntries,
			Args: bb,
		}, tx)
		if err != nil {
			return err
		}
		_, err = tx.Exec("UPDATE monitored_logs SET processed = $1 WHERE url = $2", sth.TreeSize, md.URL)
		if err != nil {
			return err
		}
	}
	return nil
}
