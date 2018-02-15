package main

import (
	"encoding/json"
	"errors"
	"log"

	que "github.com/bgentry/que-go"
	"github.com/jackc/pgx"
)

type GetEntriesConf struct {
	URL        string
	Start, End int64
}

const (
	KeyGetEntries = "get_entries"
)

func GetEntries(qc *que.Client, logger *log.Logger, job *que.Job, tx *pgx.Tx) error {
	var md GetEntriesConf
	err := json.Unmarshal(job.Args, &md)
	if err != nil {
		return err
	}

	return errors.New("not im")
}
