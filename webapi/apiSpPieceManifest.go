package main

import (
	"bytes"
	"net/http"
	"text/template"

	"github.com/data-preservation-programs/spade/apitypes"
	"github.com/data-preservation-programs/spade/internal/app"
	filabi "github.com/filecoin-project/go-state-types/abi"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"golang.org/x/xerrors"
)

func apiSpPieceManifest(c echo.Context) error {
	ctx, ctxMeta := unpackAuthedEchoContext(c)

	pu := c.QueryParams().Get("proposal")
	if pu == "" {
		return retFail(
			c,
			apitypes.ErrInvalidRequest,
			"A `proposal` UUID parameter must be supplied to this call",
		)
	}
	if _, err := uuid.Parse(pu); err != nil {
		return retFail(
			c,
			apitypes.ErrInvalidRequest,
			"The supplied `proposal` parameter '%s' is not a valid UUID: %s",
			pu,
			err,
		)
	}

	pcs := make([]struct {
		AggLog2Size int    `db:"agg_log2size"`
		AggPCidV1   string `db:"agg_pcid_v1"`
		SegPCidV2   string `db:"seg_pcid_v2"`
		UrlTemplate string
	}, 0, 8<<10)

	if err := pgxscan.Select(
		ctx,
		ctxMeta.Db[app.DbMain],
		&pcs,
		`
		SELECT
				ap.piece_cid AS agg_pcid_v1,
				ap.piece_log2_size AS agg_log2size,
				sp.piece_cid AS seg_pcid_v2,
				t.tenant_meta->'bulk_piece_source'->>'url_template' AS url_template
			FROM spd.piece_segments ps
			JOIN spd.pieces ap USING ( piece_id )
			JOIN spd.pieces sp ON ( ps.segment_id = sp.piece_id )
			JOIN spd.proposals pr ON ( pr.piece_id = ps.piece_id )
			JOIN spd.clients cl USING ( client_id )
			JOIN spd.tenants t USING ( tenant_id )
		WHERE
			(ap.piece_meta->'is_frc58_segmented')::bool
				AND
			pr.proposal_uuid = $1
				AND
			-- ensure we only display SPs own proposals, no list-sharing
			pr.provider_id = $2
				AND
			-- only pending proposals
			pr.proposal_delivered IS NOT NULL AND pr.proposal_failstamp = 0 AND pr.activated_deal_id IS NULL

		-- ordering is critical
		ORDER BY ps.position
		`,
		pu,
		ctxMeta.authedActorID,
	); err != nil {
		return cmn.WrErr(err)
	}

	if len(pcs) == 0 {
		return retFail(
			c,
			apitypes.ErrInvalidRequest,
			"no results for proposal UUID '%s': either it does not exist, is too recent, does not belong to %s or is not segmented",
			pu,
			ctxMeta.authedActorID.AsFilAddr().String(),
		)
	}

	utText := pcs[0].UrlTemplate
	if utText == "" {
		return xerrors.New("do not know how to handle segments without a URL template yet...")
	}
	ut, err := template.New("url").Parse(utText)
	if err != nil {
		return cmn.WrErr(err)
	}

	aggCP, err := fil.CommPFromPieceInfo(filabi.PieceInfo{
		Size:     1 << pcs[0].AggLog2Size,
		PieceCID: cid.MustParse(pcs[0].AggPCidV1),
	})
	if err != nil {
		return cmn.WrErr(err)
	}

	resp := apitypes.ResponsePieceManifestFR58{
		AggPCidV2: aggCP.PCidV2().String(),
		Segments:  make([]apitypes.Segment, len(pcs)),
	}

	for i := range pcs {
		u := new(bytes.Buffer)
		if err := ut.Execute(u, pcs[i]); err != nil {
			return cmn.WrErr(err)
		}
		resp.Segments[i].PCidV2 = pcs[i].SegPCidV2
		resp.Segments[i].Sources = []string{u.String()}
	}

	return retPayloadAnnotated(
		c,
		http.StatusOK,
		0,
		resp,
		"",
	)
}
