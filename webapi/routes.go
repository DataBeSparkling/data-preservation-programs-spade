package main

import "github.com/labstack/echo/v4"

// This lists in one place all recognized routes & parameters
// FIXME - we should make an openapi or something for this...
func registerRoutes(e *echo.Echo) {
	spRoutes := e.Group("/sp", spidAuth)

	//
	// /status produces human and machine readable information about the system and the currently-authenticated SP
	//
	// Recognized parameters: none
	//
	spRoutes.GET("/status", apiSpStatus)

	//
	// /eligible_pieces produces a listing of PieceCIDs that a storage provider is eligible to receive a deal for.
	// The list is dynamic and offers a near-real-time view specific to the authenticated SP answering:
	// "What can I reserve/request right this moment"
	//
	// Recognized parameters:
	//
	// - limit = <integer>
	//   How many results to return at most
	//   default=listEligibleDefaultSize
	//
	// - tenant = <integer>
	//   Restrict the list to only pieces claimed by this numeric TenantID. No restriction if unspecified.
	//
	// - include-sourceless = <boolean>
	//   When true the result includes eligible pieces without any known sources. Such pieces are omitted by default.
	//
	spRoutes.GET("/eligible_pieces", apiSpListEligible)

	//
	// /pending_proposals produces a list of current outstanding reservations, recent errors and various statistics.
	//
	// Recognized parameters: none
	//
	spRoutes.GET("/pending_proposals", apiSpListPendingProposals)

	//
	// /piece_manifest produces a manifest for a segmented piece. You need a reservation proposal UUID to call this.
	//
	// Required parameters:
	//
	// - proposal = <uuid>
	//
	spRoutes.GET("/piece_manifest", apiSpPieceManifest)

	//
	// /invoke is the sole mutating (POST) method, with several recognized RPC-calls:
	//
	// - reserve_piece: used to request a deal proposal (and thus reservation) for a specific
	//   PieceCID. The call will fail with HTTP 403 + a corresponding internal error code if the SP
	//   is not eligible to receive a deal for this PieceCID. On success a deal proposal is queued and
	//   delivered to the SP by a periodic task, executed outside of this webapp.
	//
	//
	spRoutes.POST("/invoke", apiSpInvoke)
	spRoutes.GET("/invoke", retInvalidRoute)
}
