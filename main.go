package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
	"github.com/go-pg/pg"
	"github.com/gorilla/websocket"

	"github.com/bgentry/que-go"
	"github.com/jackc/pgx"
)

type Certificate struct {
	Subject struct {
		Aggregated       string `json:"aggregated"`
		Country          string `json:"C"`
		State            string `json:"ST"`
		Locality         string `json:"L"`
		Organisation     string `json:"O"`
		OrganisationUnit string `json:"OU"`
		CommonName       string `json:"CN"`
	} `json:"subject"`
	Extensions struct {
		KeyUsage               string `json:"keyUsage"`
		ExtendedKeyUsage       string `json:"extendedKeyUsage"`
		BasicConstraints       string `json:"basicConstraints"`
		SubjectKeyIdentifier   string `json:"subjectKeyIdentifier"`
		AuthorityKeyIdentifier string `json:"authorityKeyIdentifier"`
		AuthorityInfoAccess    string `json:"authorityInfoAccess"`
		SubjectAltName         string `json:"subjectAltName"`
		CertificatePolicies    string `json:"certificatePolicies"`
		CRLDistributionPoints  string `json:"crlDistributionPoints"`
	} `json:"extensions"`
	NotBefore    float64  `json:"not_before"`
	NotAfter     float64  `json:"not_after"`
	SerialNumber string   `json:"serial_number"`
	Fingerprint  string   `json:"fingerprint"`
	DER          string   `json:"as_der"`
	AllDomains   []string `json:"all_domains"`
}

type Message struct {
	MessageType string `json:"message_type"`
	Data        struct {
		UpdateType string        `json:"update_type"`
		LeafCert   Certificate   `json:"leaf_cert"`
		Chain      []Certificate `json:"chain"`
	} `json:data`
	CertIndex int64   `json:"cert_index"`
	Seen      float64 `json:"seen"`
	Source    struct {
		URL  string `json:"url"`
		Name string `json:"name"`
	} `json:"source"`
}

func (s *server) StreamAndLog() {
	for {
		c, _, err := websocket.DefaultDialer.Dial("wss://certstream.calidog.io", nil)

		if err != nil {
			log.Println("Error connecting to certstream! Sleeping a few seconds and reconnecting... ")
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			var message Message
			err := c.ReadJSON(&message)
			if err != nil {
				log.Println("Error reading message")
				break
			}

			if message.MessageType == "certificate_update" {
				err = s.gotCert(&message.Data.LeafCert)
				if err != nil {
					log.Println("error having cert", err)
				}
			}
		}

		c.Close()
	}
}

type CertObserved struct {
	Domain       string `sql:",pk"`
	SerialNumber string `sql:",pk"`
	Seen         time.Time
}

type server struct {
	DB *pg.DB
}

// Return a database object, using the CloudFoundry environment data
func postgresCredsFromCF() (map[string]interface{}, error) {
	appEnv, err := cfenv.Current()
	if err != nil {
		return nil, err
	}

	dbEnv, err := appEnv.Services.WithTag("postgres")
	if err != nil {
		return nil, err
	}

	if len(dbEnv) != 1 {
		return nil, errors.New("expecting 1 database")
	}

	return dbEnv[0].Credentials, nil
}

func (s *server) gotCert(cert *Certificate) error {
	interesting := false
	for _, dom := range cert.AllDomains {
		if strings.HasSuffix(dom, ".com") {
			interesting = true
			break
		}
	}

	if interesting {
		for _, dom := range cert.AllDomains {
			err := s.DB.Insert(&CertObserved{
				Domain:       dom,
				SerialNumber: cert.SerialNumber,
				Seen:         time.Now(),
			})
			if err != nil {
				// we will often run two of us, to ensure we don't miss anything, so we expect a lot of duplicate errors
				if isErrDuplicateKey(err) {
					continue
				}
				return err
			}
		}
	}

	return nil
}

func isErrRelationAlreadyExists(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "ERROR #42P07")
}

func isErrDuplicateKey(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "ERROR #23505")
}

func isErrNotFound(err error) bool {
	return err != nil && err.Error() == "pg: no rows in result set"
}

const (
	KnownLogsURL = "https://www.gstatic.com/ct/log_list/log_list.json"
	WorkerCount  = 10

	MutexJobUpdate = 100
)

func initQueStructures(creds map[string]interface{}) error {
	pgxPool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Database: creds["name"].(string),
			User:     creds["username"].(string),
			Password: creds["password"].(string),
			Host:     creds["host"].(string),
			Port:     uint16(creds["port"].(float64)),
		},
	})
	defer pgxPool.Close()
	if err != nil {
		return err
	}

	_, err = pgxPool.Exec(`CREATE TABLE IF NOT EXISTS que_jobs
		(
		  priority    smallint    NOT NULL DEFAULT 100,
		  run_at      timestamptz NOT NULL DEFAULT now(),
		  job_id      bigserial   NOT NULL,
		  job_class   text        NOT NULL,
		  args        json        NOT NULL DEFAULT '[]'::json,
		  error_count integer     NOT NULL DEFAULT 0,
		  last_error  text,
		  queue       text        NOT NULL DEFAULT '',
 
		  CONSTRAINT que_jobs_pkey PRIMARY KEY (queue, priority, run_at, job_id)
		);
 
		COMMENT ON TABLE que_jobs IS '3';`)
	if err != nil {
		return err
	}

	return nil
}

func initORMTables(db *pg.DB) error {
	for _, model := range []interface{}{
		&CertObserved{},
		&MonitoredLog{},
		&CronMetadata{},
	} {
		err := db.CreateTable(model, nil)
		if err != nil {
			// swallow this one, we'll see if on every startup
			if isErrRelationAlreadyExists(err) {
				continue
			}
			return err
		}
	}

	// Create dummy records if needed, so that we can lock on them
	// err := db.Insert(&CronMetadata{
	// 	ID: KeyUpdateLogs,
	// })
	// if err != nil {
	// 	if !isErrDuplicateKey(err) {
	// 		return err
	// 	}
	// }

	return nil
}

func main() {
	creds, err := postgresCredsFromCF()
	if err != nil {
		log.Fatal(err)
	}

	err = initQueStructures(creds)
	if err != nil {
		log.Fatal(err)
	}

	pg := pg.Connect(&pg.Options{
		Addr:     fmt.Sprintf("%s:%d", creds["host"].(string), int(creds["port"].(float64))),
		Database: creds["name"].(string),
		User:     creds["username"].(string),
		Password: creds["password"].(string),
	})

	err = initORMTables(pg)
	if err != nil {
		log.Fatal(err)
	}

	pgxPool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Database: creds["name"].(string),
			User:     creds["username"].(string),
			Password: creds["password"].(string),
			Host:     creds["host"].(string),
			Port:     uint16(creds["port"].(float64)),
		},
		AfterConnect: que.PrepareStatements,
	})
	if err != nil {
		log.Fatal(err)
	}

	qc := que.NewClient(pgxPool)
	workers := que.NewWorkerPool(qc, que.WorkMap{
		KeyUpdateLogs: (&LogUpdater{
			DB:     pg,
			Logger: log.New(os.Stderr, "LOGUPDATER", log.LstdFlags),
			URL:    KnownLogsURL,
		}).Run,
	}, WorkerCount)

	// Prepare a shutdown function
	shutdown := func() {
		defer pg.Close()
		defer pgxPool.Close()

		workers.Shutdown()
	}

	// Normal exit
	defer shutdown()

	// Or via signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received %v, starting shutdown...", sig)
		shutdown()
		log.Println("Shutdown complete")
		os.Exit(0)
	}()

	go workers.Start()

	for i := 0; i < 3; i++ {
		err = qc.Enqueue(&que.Job{
			Args: []byte("{}"),
			Type: KeyUpdateLogs,
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Started up... waiting for ctrl-C.")
	select {}

	// s := &server{
	// 	DB: db,
	// }
	// err = s.Init()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// s.updateLogs(KnownLogsURL)
	// //s.StreamAndLog()
}
