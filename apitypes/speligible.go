package apitypes

// ResponsePiecesEligible is the response payload returned by the .../sp/eligible_pieces endpoint
type ResponsePiecesEligible []*Piece

type Piece struct {
	PieceCid         string `json:"piece_cid"`
	PaddedPieceSize  uint64 `json:"padded_piece_size"`
	ClaimingTenant   int16  `json:"tenant_id"`
	TenantPolicyCid  string `json:"tenant_policy_cid"`
	SampleReserveCmd string `json:"sample_reserve_cmd,omitempty"`
}

func (ResponsePiecesEligible) is() isResponsePayload { return isResponsePayload{} }
