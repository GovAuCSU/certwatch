package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	que "github.com/bgentry/que-go"
	ct "github.com/google/certificate-transparency-go"
	ctclient "github.com/google/certificate-transparency-go/client"
	ctjsonclient "github.com/google/certificate-transparency-go/jsonclient"
	"github.com/google/certificate-transparency-go/tls"
	ctx509 "github.com/google/certificate-transparency-go/x509"
	"github.com/jackc/pgx"
)

type GetEntriesConf struct {
	URL        string
	Start, End uint64 // end is exclusive
}

const (
	KeyGetEntries = "get_entries"

	DomainSuffix = ".gov.au"
)

func GetEntries(qc *que.Client, logger *log.Logger, job *que.Job, tx *pgx.Tx) error {
	var md GetEntriesConf
	err := json.Unmarshal(job.Args, &md)
	if err != nil {
		return err
	}

	lc, err := ctclient.New(fmt.Sprintf("https://%s", md.URL), http.DefaultClient, ctjsonclient.Options{Logger: logger})
	if err != nil {
		return err
	}

	entries, err := lc.GetRawEntries(context.Background(), int64(md.Start), int64(md.End)-1)
	if err != nil {
		return err
	}

	idx := md.Start
	for _, e := range entries.Entries {
		var leaf ct.MerkleTreeLeaf
		_, err := tls.Unmarshal(e.LeafInput, &leaf)
		if err != nil {
			return err
		}
		if leaf.LeafType != ct.TimestampedEntryLeafType {
			return fmt.Errorf("unknown leaf type: %v", leaf.LeafType)
		}
		if leaf.TimestampedEntry == nil {
			return errors.New("nil timestamped entry")
		}
		var cert *ctx509.Certificate
		switch leaf.TimestampedEntry.EntryType {
		case ct.X509LogEntryType:
			// swallow errors, as this parser is will still return partially valid certs, which are good enough for our analysis
			cert, _ = leaf.X509Certificate()
			if cert == nil {
				logger.Printf("cannot parse cert %s (%d), ignoring.", md.URL, idx)
			}
		case ct.PrecertLogEntryType:
			// swallow errors, as this parser is will still return partially valid certs, which are good enough for our analysis
			cert, _ = leaf.Precertificate()
			if cert == nil {
				logger.Printf("cannot parse precert %s (%d), ignoring.", md.URL, idx)
			}
		default:
			return fmt.Errorf("unknown leaf type: %v", leaf.LeafType)
		}

		doms := make(map[string]bool)

		if cert != nil {
			if strings.HasSuffix(cert.Subject.CommonName, DomainSuffix) {
				doms[cert.Subject.CommonName] = true
			}

			for _, name := range cert.DNSNames {
				if strings.HasSuffix(name, DomainSuffix) {
					doms[name] = true
				}
			}
		}

		if len(doms) != 0 {
			// We care more about the certs, than the logs, so let's wipe out the timestamp, so that
			// multiple logs reporting the same cert, only store one.
			// Now note that we'll still get some dupes, as a pre-cert and cert will appear as two different
			// things. TODO...
			leaf.TimestampedEntry.Timestamp = 0

			certToStore, err := tls.Marshal(leaf)
			if err != nil {
				return err
			}

			kh := sha256.Sum256(certToStore)
			_, err = tx.Exec("INSERT INTO cert_store (key, leaf) VALUES ($1, $2) ON CONFLICT DO NOTHING", kh[:], certToStore)
			if err != nil {
				return err
			}

			for dom := range doms {
				_, err = tx.Exec("INSERT INTO cert_index (key, domain) VALUES ($1, $2) ON CONFLICT DO NOTHING", kh[:], dom)
				if err != nil {
					return err
				}
			}
		}

		idx++
	}

	// Did we fall short of the amount we needed?
	if idx < md.End {
		midPoint := idx + ((md.End - idx) / 2)

		if idx < midPoint {
			bb, err := json.Marshal(&GetEntriesConf{
				URL:   md.URL,
				Start: idx,
				End:   midPoint,
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
		}

		if midPoint < md.End {
			bb, err := json.Marshal(&GetEntriesConf{
				URL:   md.URL,
				Start: midPoint,
				End:   md.End,
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
		}
	}

	return nil
}
