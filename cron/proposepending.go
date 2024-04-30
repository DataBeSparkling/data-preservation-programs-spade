package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/data-preservation-programs/spade/internal/app"
	"github.com/data-preservation-programs/spade/internal/filtypes"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/google/uuid"
	"github.com/ribasushi/go-toolbox-interplanetary/lp2p"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	filaddr "github.com/filecoin-project/go-address"
	filmarket "github.com/filecoin-project/go-state-types/builtin/v9/market"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"
)

type proposalPending struct {
	ProposalUUID      uuid.UUID
	ProposalPayload   filmarket.DealProposal
	ProposalSignature filcrypto.Signature
	ProposalCid       string
	PeerID            *lp2p.PeerID
	Multiaddrs        []string
}
type proposalsPerSP map[filaddr.Address][]proposalPending

type runTotals struct {
	proposals       int
	uniqueProviders int
	delivered120    *int32
	timedout        *int32
	failed          *int32
}

var (
	spProposalSleep int
	proposalTimeout int
	perSpTimeout    int
)
var proposePending = &ufcli.Command{
	Usage: "Propose pending reservations to providers",
	Name:  "propose-pending",
	Flags: []ufcli.Flag{
		&ufcli.IntFlag{
			Name:        "sleep-between-proposals",
			Usage:       "Amount of seconds to wait between proposals to same SP",
			Value:       3,
			Destination: &spProposalSleep,
		},
		&ufcli.IntFlag{
			Name:        "proposal-timeout",
			Usage:       "Amount of seconds before aborting a specific proposal",
			Value:       90,
			Destination: &proposalTimeout,
		},
		&ufcli.IntFlag{
			Name:        "per-sp-timeout",
			Usage:       "Amount of seconds proposals for specific SP could take in total",
			Value:       270, // 4.5 mins
			Destination: &perSpTimeout,
		},
	},
	Action: func(cctx *ufcli.Context) error {
		ctx, log, db, _ := app.UnpackCtx(cctx.Context)

		tot := runTotals{
			delivered120: new(int32),
			timedout:     new(int32),
			failed:       new(int32),
		}
		defer func() {
			log.Infow("summary",
				"uniqueProviders", tot.uniqueProviders,
				"proposals", tot.proposals,
				"successfulV120", atomic.LoadInt32(tot.delivered120),
				"failed", atomic.LoadInt32(tot.failed),
				"timedout", atomic.LoadInt32(tot.timedout),
			)
		}()

		pending := make([]proposalPending, 0, 2048)
		if err := pgxscan.Select(
			ctx,
			db,
			&pending,
			`
			SELECT
					pr.proposal_uuid,
					pr.proposal_meta->'filmarket_proposal' AS proposal_payload,
					pr.proposal_meta->'signature' AS proposal_signature,
					pr.proposal_meta->>'signed_proposal_cid' AS proposal_cid,
					pi.info->'peerid' AS peer_id,
					pi.info->'multiaddrs' AS multiaddrs
				FROM spd.proposals pr
				JOIN spd.pieces p USING ( piece_id )
				LEFT JOIN spd.providers_info pi USING ( provider_id )
			WHERE
				proposal_delivered IS NULL
					AND
				signature_obtained IS NOT NULL
					AND
				proposal_failstamp = 0
			ORDER BY entry_created
			`,
		); err != nil {
			return cmn.WrErr(err)
		}

		props := make(proposalsPerSP, 4)
		for _, p := range pending {

			if p.PeerID == nil || len(p.Multiaddrs) == 0 {
				if _, err := db.Exec(
					context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
					`
					UPDATE spd.proposals SET
						proposal_failstamp = spd.big_now(),
						proposal_meta = JSONB_SET( proposal_meta, '{ failure }', TO_JSONB( 'provider not dialable: insufficient information published on chain'::TEXT ) )
					WHERE
						proposal_uuid = $1
					`,
					p.ProposalUUID,
				); err != nil {
					return cmn.WrErr(err)
				}
				continue
			}

			props[p.ProposalPayload.Provider] = append(props[p.ProposalPayload.Provider], p)
			tot.proposals++
		}
		tot.uniqueProviders = len(props)

		nodeHost, _, err := lp2p.NewPlainNodeTCP(time.Duration(proposalTimeout) * time.Second)
		if err != nil {
			return cmn.WrErr(err)
		}
		defer func() {
			if err := nodeHost.Close(); err != nil {
				log.Warnf("unexpected error shutting down node %s: %s", nodeHost.ID().String(), err)
			}
		}()

		eg, ctx := errgroup.WithContext(ctx)
		for sp := range props {
			sp := sp
			eg.Go(func() error { return proposeToSp(ctx, nodeHost, props[sp], tot) })
		}
		return eg.Wait()
	},
}

func proposeToSp(ctx context.Context, nodeHost lp2p.Host, props []proposalPending, tot runTotals) error {

	dealCount := len(props)
	if dealCount == 0 {
		return nil
	}

	ctx, log, db, _ := app.UnpackCtx(ctx)
	sp := props[0].ProposalPayload.Provider

	jobDesc := fmt.Sprintf("proposing %d storage contracts to %s", dealCount, sp)
	var delivered, failed, timedout int
	log.Info("START " + jobDesc)
	t0 := time.Now()
	defer func() {
		log.Infof(
			"END %s, out of %d proposals: %d succeeded, %d failed, %d timed out, took %s",
			jobDesc,
			dealCount,
			delivered, failed, timedout,
			time.Since(t0).String(),
		)
	}()

	spAI, err := lp2p.AssembleAddrInfo(props[0].PeerID, props[0].Multiaddrs)
	if err != nil {
		return err
	}

	// do everything in the same loop, even the lp2p dial, so that we can
	// reuse the db-update code either way
	for i, p := range props {

		// some SPs take *FOREVER* to respond ( 40+ seconds )
		// Cap processing, so that the rest of the queue isn't held up
		// ( they will restart from where they left off on next round )
		if time.Since(t0) >= time.Duration(perSpTimeout+spProposalSleep)*time.Second {
			return nil
		}

		// disconnect and wait a bit between deliveries
		if i != 0 {
			if err := nodeHost.Network().ClosePeer(*spAI.PeerID); err != nil {
				log.Warnf("unexpected error disconnecting from SP %s: %s", sp.String(), err)
			}
			select {
			case <-ctx.Done():
				return nil // cancellation is not an error here
			case <-time.After(time.Duration(spProposalSleep) * time.Second):
			}
		}

		var resp filtypes.StorageProposalV120Response
		tCtx, tCtxCancel := context.WithTimeout(ctx, time.Duration(proposalTimeout)*time.Second)
		proposalTook, proposalErr := lp2p.ExecCborRPC(
			tCtx,
			nodeHost,
			spAI,
			filtypes.StorageProposalV120,
			&filtypes.StorageProposalV12xParams{
				IsOffline:          true, // not negotiable: out-of-band-transfers forever
				DealUUID:           p.ProposalUUID,
				RemoveUnsealedCopy: false, // potentially allow tenants to request different defaults
				SkipIPNIAnnounce:   false, // in the murky future
				ClientDealProposal: filmarket.ClientDealProposal{
					Proposal:        p.ProposalPayload,
					ClientSignature: p.ProposalSignature,
				},
				// there is no "DataRoot" - always set to the PieceCID itself as per
				// https://filecoinproject.slack.com/archives/C03AQ3QAUG1/p1662622159003079?thread_ts=1662552800.028649&cid=C03AQ3QAUG1
				DealDataRoot: p.ProposalPayload.PieceCID,
			},
			&resp,
		)
		tCtxCancel()

		// set the response error if we got it successfully
		if proposalErr == nil && !resp.Accepted {
			proposalErr = xerrors.New(resp.Message)
		}

		// set the localpeerid and timing info
		ptj, err := json.Marshal(proposalTook)
		if err != nil {
			return cmn.WrErr(err)
		}

		if _, err := db.Exec(
			context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
			`
			UPDATE spd.proposals SET
				proposal_meta = proposal_meta || $2::JSONB
			WHERE proposal_uuid = $1
			`,
			p.ProposalUUID,
			ptj,
		); err != nil {
			return cmn.WrErr(err)
		}

		// we did it!
		if proposalErr == nil {

			delivered++
			atomic.AddInt32(tot.delivered120, 1)

			if _, err := db.Exec(
				context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
				`
				UPDATE spd.proposals SET
					proposal_delivered = NOW()
				WHERE
					proposal_uuid = $1
				`,
				p.ProposalUUID,
			); err != nil {
				return cmn.WrErr(err)
			}
		} else {
			log.Error(proposalErr)

			didTimeout := errors.Is(proposalErr, context.DeadlineExceeded)
			if didTimeout {
				timedout++
				atomic.AddInt32(tot.timedout, 1)
			} else {
				failed++
				atomic.AddInt32(tot.failed, 1)
			}

			if _, err := db.Exec(
				context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
				`
				UPDATE spd.proposals SET
					proposal_failstamp = spd.big_now(),
					proposal_meta = JSONB_STRIP_NULLS(
						JSONB_SET(
							proposal_meta,
							'{ failure }',
							TO_JSONB( $2::TEXT )
						)
					)
				WHERE
					proposal_uuid = $1
				`,
				p.ProposalUUID,
				proposalErr.Error(),
			); err != nil {
				return cmn.WrErr(err)
			}

			// in case of a timeout or connection failure: bail after failing just one proposal, retry next time
			if didTimeout {
				return nil
			}
		}
	}

	return nil
}
