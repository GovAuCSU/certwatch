package main

import (
	"errors"
	"log"

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
	return errors.New("notimplementedyet")
}
