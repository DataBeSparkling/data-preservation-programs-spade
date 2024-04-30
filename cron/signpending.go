package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/data-preservation-programs/spade/internal/app"
	filaddr "github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	filmarket "github.com/filecoin-project/go-state-types/builtin/v9/market"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jsign/go-filsigner/wallet"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
)

type signTotals struct {
	signed *int32
	failed *int32
}

type knownWallet struct {
	robust filaddr.Address
	key    string
}

var signPending = &ufcli.Command{
	Usage: "Sign pending deal proposals",
	Name:  "sign-pending",
	Flags: []ufcli.Flag{},
	Action: func(cctx *ufcli.Context) error {
		ctx, log, db, gctx := app.UnpackCtx(cctx.Context)

		totals := signTotals{
			signed: new(int32),
			failed: new(int32),
		}
		knownWallets := make(map[filaddr.Address]knownWallet)
		defer func() {
			log.Infow("summary",
				"uniqueWallets", len(knownWallets),
				"successful", atomic.LoadInt32(totals.signed),
				"failed", atomic.LoadInt32(totals.failed),
			)
		}()

		type signaturePending struct {
			ProposalUUID    string
			ProposalPayload filmarket.DealProposal
		}

		pending := make([]signaturePending, 0, 128)
		if err := pgxscan.Select(
			ctx,
			db,
			&pending,
			`
			SELECT
					pr.proposal_uuid,
					pr.proposal_meta->'filmarket_proposal' AS proposal_payload
				FROM spd.proposals pr
			WHERE
				signature_obtained IS NULL
					AND
				proposal_failstamp = 0
			`,
		); err != nil {
			return cmn.WrErr(err)
		}

		if len(pending) == 0 {
			return nil
		}

		for _, p := range pending {

			if _, found := knownWallets[p.ProposalPayload.Client]; !found {
				var kw knownWallet

				if p.ProposalPayload.Client.Protocol() == filaddr.ID {
					robust, err := gctx.LotusAPI.StateAccountKey(ctx, p.ProposalPayload.Client, fil.LotusTSK{})
					if err != nil {
						return cmn.WrErr(err)
					}
					kw.robust = robust
				} else {
					kw.robust = p.ProposalPayload.Client
				}

				hm, err := os.UserHomeDir()
				if err != nil {
					return cmn.WrErr(err)
				}
				fh, err := os.Open(fmt.Sprintf("%s/.keystore/%s.key", hm, kw.robust.String()))
				if err != nil {
					return cmn.WrErr(err)
				}
				defer fh.Close()
				k, err := bufio.NewReader(fh).ReadString('\n')
				if err != nil {
					return cmn.WrErr(err)
				}
				kw.key = strings.TrimSpace(k)

				knownWallets[p.ProposalPayload.Client] = kw
			}

			raw, err := cborutil.Dump(&p.ProposalPayload)
			if err != nil {
				return cmn.WrErr(err)
			}

			var sig *filcrypto.Signature
			sig, err = wallet.WalletSign(knownWallets[p.ProposalPayload.Client].key, raw)
			if err != nil {
				atomic.AddInt32(totals.failed, 1)
				log.Error(err)
				return cmn.WrErr(err)
			}

			propNode, err := cborutil.AsIpld(&filmarket.ClientDealProposal{
				Proposal:        p.ProposalPayload,
				ClientSignature: *sig,
			})
			if err != nil {
				return cmn.WrErr(err)
			}

			if _, err := db.Exec(
				ctx,
				`
				UPDATE spd.proposals SET
					signature_obtained = NOW(),
					proposal_meta = JSONB_SET(
						JSONB_SET(
							proposal_meta,
							'{ signature }',
							$1
						),
						'{ signed_proposal_cid }',
						TO_JSONB( $2::TEXT )
					)
				WHERE proposal_uuid = $3
				`,
				sig,
				propNode.Cid().String(),
				p.ProposalUUID,
			); err != nil {
				return cmn.WrErr(err)
			}

			atomic.AddInt32(totals.signed, 1)
		}

		return nil
	},
}
