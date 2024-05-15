package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconf "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/data-preservation-programs/spade/internal/app"
	"github.com/filecoin-project/go-data-segment/datasegment"
	filabi "github.com/filecoin-project/go-state-types/abi"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v4"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
	"golang.org/x/xerrors"
)

type BulkPieceSource struct {
	Type        string
	Config      string
	PathParts   []string `json:"path_parts"`
	Flags       map[string]bool
	URLTemplate string `json:"url_template"`
}

type Aggregate struct {
	AggCommP   WrCommP   `json:"aggregate"`
	PieceList  []WrCommP `json:"pieces"`
	Collection string
}

var bulkPiecePoll = &ufcli.Command{
	Usage: "Query newly available pieces from configured tenants",
	Name:  "bulk-piece-poll",
	Flags: []ufcli.Flag{
		&ufcli.UintFlag{
			Name:  "skip-entries-aged-days",
			Usage: "Only query pieces posted in the last N days",
			Value: 2,
		},
	},
	Action: func(cctx *ufcli.Context) (defErr error) {
		ctx, log, db, _ := app.UnpackCtx(cctx.Context)

		// if everything went well: refresh the matviews
		defer func() {
			if defErr == nil {
				defErr = db.BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
					return app.RefreshMatviews(ctx, tx)
				})
			}
		}()

		bps := make([]struct {
			TenantID        int32
			BulkPieceSource BulkPieceSource
		}, 0, 4)

		if err := pgxscan.Select(
			ctx,
			db,
			&bps,
			`
			SELECT tenant_meta->'bulk_piece_source' AS bulk_piece_source, tenant_id
				FROM spd.tenants
			WHERE tenant_meta->'bulk_piece_source' IS NOT NULL
			`,
		); err != nil {
			return cmn.WrErr(err)
		}

		for _, b := range bps {
			if b.BulkPieceSource.Type != "s3" {
				return fmt.Errorf("unsupported bulk source type '%s' for tenantd %d", b.BulkPieceSource.Type, b.TenantID)
			}

			cfg, err := awsconf.LoadDefaultConfig(
				ctx,
				awsconf.WithSharedConfigProfile(b.BulkPieceSource.Config),
				awsconf.WithRegion(b.BulkPieceSource.PathParts[0]),
			)
			if err != nil {
				return cmn.WrErr(err)
			}
			s3api := awss3.NewFromConfig(cfg)
			bucket := aws.String(b.BulkPieceSource.PathParts[1])

			for d := time.Duration(cctx.Uint("skip-entries-aged-days")); d >= 0; d-- {
				var dayCount int
				pref := time.Now().Add(-24 * time.Hour * d).Format(time.DateOnly)

				var nextPage *string
				for {
					ls, err := s3api.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
						Bucket:            bucket,
						Prefix:            aws.String(pref),
						ContinuationToken: nextPage,
					})
					if err != nil {
						return cmn.WrErr(err)
					}

					for _, e := range ls.Contents {
						res, err := s3api.GetObject(ctx, &awss3.GetObjectInput{
							Key:    e.Key,
							Bucket: bucket,
						})
						if err != nil {
							return cmn.WrErr(err)
						}
						defer res.Body.Close()

						var agg Aggregate
						if err := json.NewDecoder(res.Body).Decode(&agg); err != nil {
							return cmn.WrErr(err)
						}

						if b.BulkPieceSource.Flags == nil || !b.BulkPieceSource.Flags["is_frc58"] {
							return xerrors.New("do not know how to handle non-frc58 bulksources yet")
						}

						// check validity
						//
						pis := make([]filabi.PieceInfo, len(agg.PieceList))
						for i := range agg.PieceList {
							pis[i] = agg.PieceList[i].PieceInfo()
						}

						aggObj, err := datasegment.NewAggregate(agg.AggCommP.PieceInfo().Size, pis)
						if err != nil {
							return cmn.WrErr(err)
						}
						aggReifiedPcidV1, err := aggObj.PieceCID()
						if err != nil {
							return cmn.WrErr(err)
						}

						if aggReifiedPcidV1 != agg.AggCommP.PCidV1() {
							return xerrors.Errorf("supplied list of %d pieces does not aggregate with the expected PCidV1 %s, got %s instead", len(agg.PieceList), agg.AggCommP.PCidV1(), aggReifiedPcidV1)
						}

						// we are good, push into db
						// FUTURE TODO - add an "already exists" counter (for now just blindly mash everything in, reproducibility ftw)
						if err := db.BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {

							if _, err := tx.Exec(
								ctx,
								`INSERT INTO spd.pieces ( piece_cid, piece_log2_size, piece_meta ) VALUES ( $1, $2, $3 ) ON CONFLICT DO NOTHING`,
								agg.AggCommP.PCidV1(), // use v1 for legacy reasons
								agg.AggCommP.PieceLog2Size(),
								`{ "is_frc58_segmented": true }`,
							); err != nil {
								return cmn.WrErr(err)
							}

							for i, s := range agg.PieceList {
								if _, err := tx.Exec(
									ctx,
									`INSERT INTO spd.pieces ( piece_cid, piece_log2_size ) VALUES ( $1, $2 ) ON CONFLICT DO NOTHING`,
									s.PCidV2(), // use v2 for segments
									s.PieceLog2Size(),
								); err != nil {
									return cmn.WrErr(err)
								}

								if _, err := tx.Exec(
									ctx,
									`
									INSERT INTO spd.piece_segments ( piece_id, segment_id, position ) VALUES (
										 ( SELECT piece_id FROM spd.pieces WHERE piece_cid = $1 ),
										 ( SELECT piece_id FROM spd.pieces WHERE piece_cid = $2 ),
										 $3
									) ON CONFLICT DO NOTHING
									`,
									agg.AggCommP.PCidV1(),
									s.PCidV2(),
									i,
								); err != nil {
									return cmn.WrErr(err)
								}
							}

							if _, err := tx.Exec(
								ctx,
								`
								INSERT INTO spd.tenants_pieces ( piece_id, dataset_id, tenant_id ) VALUES (
									 ( SELECT piece_id FROM spd.pieces WHERE piece_cid = $1 ),
									 ( SELECT dataset_id FROM spd.datasets WHERE tenant_id = $2 limit 1 ),
									 $2
								) ON CONFLICT DO NOTHING
								`,
								agg.AggCommP.PCidV1(),
								b.TenantID,
							); err != nil {
								return cmn.WrErr(err)
							}

							dayCount++
							return nil
						}); err != nil {
							return cmn.WrErr(err)
						}
					}
					nextPage = ls.NextContinuationToken
					if nextPage == nil {
						break
					}
				}

				log.Infof("processed %d aggregates from %s for tenant %d", dayCount, pref, b.TenantID)
			}
		}

		return nil
	},
}

type WrCommP struct {
	fil.CommP
}

func (out *WrCommP) UnmarshalJSON(b []byte) error {
	c, err := cid.Decode(string(b[1 : len(b)-1]))
	if err != nil {
		return cmn.WrErr(err)
	}
	p, err := fil.CommPFromPCidV2(c)
	if err != nil {
		return cmn.WrErr(err)
	}
	*out = WrCommP{p}
	return nil
}
