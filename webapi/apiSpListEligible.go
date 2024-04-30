package main //nolint:revive

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/data-preservation-programs/spade/apitypes"
	"github.com/data-preservation-programs/spade/internal/app"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/labstack/echo/v4"
	"github.com/ribasushi/go-toolbox/cmn"
)

func apiSpListEligible(c echo.Context) error {
	ctx, ctxMeta := unpackAuthedEchoContext(c)

	lim := uint64(listEligibleDefaultSize)
	if c.QueryParams().Has("limit") {
		var err error
		lim, err = parseUIntQueryParam(c, "limit", 1, listEligibleMaxSize)
		if err != nil {
			return retFail(c, apitypes.ErrInvalidRequest, err.Error())
		}
	}

	tenantID := int16(0) // 0 == any
	if c.QueryParams().Has("tenant") {
		tid, err := parseUIntQueryParam(c, "tenant", 1, 1<<15)
		if err != nil {
			return retFail(c, apitypes.ErrInvalidRequest, err.Error())
		}
		tenantID = int16(tid)
	}

	// how to list: start small, find setting below
	useQueryFunc := "pieces_eligible_head"

	if c.QueryParams().Has("internal-nolateral") { // secret flag to tune this in flight / figure out optimal values
		if truthyBoolQueryParam(c, "internal-nolateral") {
			useQueryFunc = "pieces_eligible_full"
		}
	} else if lim > listEligibleDefaultSize { // deduce from requested lim
		useQueryFunc = "pieces_eligible_full"
	}

	orderedPieces := make([]*struct {
		PieceID       int64
		PieceLog2Size uint8
		Tenants       []int16 `db:"tenant_ids"`
		*apitypes.Piece
	}, 0, lim+1)

	if err := pgxscan.Select(
		ctx,
		ctxMeta.Db[app.DbMain],
		&orderedPieces,
		fmt.Sprintf("SELECT * FROM spd.%s( $1, $2, $3, $4, $5 )", useQueryFunc),
		ctxMeta.authedActorID,
		lim+1, // ask for one extra, to disambiguate "there is more"
		tenantID,
		truthyBoolQueryParam(c, "include-sourceless"),
		false,
	); err != nil {
		return cmn.WrErr(err)
	}

	info := []string{
		`List of qualifying Piece CIDs.`,
		``,
		`Once you have selected a Piece CID - reserve it in the system by invoking the API as`,
		"shown in the corresponding `sample_reserve_cmd`. Within 5 minutes the reservation",
		`will activate and you will be able to see it and potential unlocked sources at:`,
		" " + curlAuthedForSP(c, ctxMeta.authedActorID, "/sp/pending_proposals", nil),
	}

	// we got more than requested - indicate that this set is large
	if uint64(len(orderedPieces)) > lim {
		orderedPieces = orderedPieces[:lim]

		exLim := lim
		if exLim < listEligibleDefaultSize {
			exLim = listEligibleDefaultSize
		}

		info = append(
			[]string{
				fmt.Sprintf(`NOTE: The complete list of entries has been TRUNCATED to the top %d.`, lim),
				"Use the 'limit' param in your API call to request more of the (possibly very large) list:",
				" " + curlAuthedForSP(c, ctxMeta.authedActorID, fmt.Sprintf("%s?limit=%d", c.Request().URL.Path, (2*exLim)/100*100), nil),
				"",
			},
			info...,
		)
	}

	ret := make(apitypes.ResponsePiecesEligible, len(orderedPieces))
	for i, p := range orderedPieces {
		sa := make(url.Values, 2)
		sa.Add("call", "reserve_piece")
		sa.Add("piece_cid", p.PieceCid)
		sa.Add("tenant_policy", app.TEMPPolicies[p.Tenants[0]])
		p.PaddedPieceSize = 1 << p.PieceLog2Size
		p.SampleReserveCmd = curlAuthedForSP(c, ctxMeta.authedActorID, "/sp/invoke", sa)
		p.ClaimingTenant = p.Tenants[0]
		p.TenantPolicyCid = app.TEMPPolicies[p.Tenants[0]]
		ret[i] = p.Piece
	}

	return retPayloadAnnotated(c, http.StatusOK, 0, ret, strings.Join(info, "\n"))
}
