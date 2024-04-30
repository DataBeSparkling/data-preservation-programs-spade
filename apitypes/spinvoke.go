package apitypes

import "time"

// ResponseDealRequest is the response payload returned by the .../sp/invoke endpoint with `call=reserve_piece`
type ResponseDealRequest struct {
	ReplicationStates []TenantReplicationState `json:"tenant_replication_states"`
	DealStartTime     *time.Time               `json:"deal_start_time,omitempty"`
	DealStartEpoch    *int64                   `json:"deal_start_epoch,omitempty"`
}

func (ResponseDealRequest) is() isResponsePayload { return isResponsePayload{} }

type DealProposal struct {
	ProposalID     string    `json:"deal_proposal_id"`
	ProposalCid    *string   `json:"deal_proposal_cid,omitempty"`
	HoursRemaining int       `json:"hours_remaining"`
	PieceSize      int64     `json:"piece_size"`
	PieceCid       string    `json:"piece_cid"`
	TenantID       int16     `json:"tenant_id"`
	TenantClient   string    `json:"tenant_client_id"`
	StartTime      time.Time `json:"deal_start_time"`
	StartEpoch     int64     `json:"deal_start_epoch"`
	ImportCmd      string    `json:"sample_import_cmd"`
	DataSources    []string  `json:"data_sources,omitempty"`
}
