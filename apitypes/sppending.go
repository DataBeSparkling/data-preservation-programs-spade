package apitypes

import "time"

func (ResponsePendingProposals) is() isResponsePayload { return isResponsePayload{} }

// ResponsePendingProposals is the response payload returned by the .../sp/pending_proposals endpoint
type ResponsePendingProposals struct {
	RecentFailures   []ProposalFailure `json:"recent_failures,omitempty"`
	PendingProposals []DealProposal    `json:"pending_proposals"`
}

type ProposalFailure struct {
	ErrorTimeStamp time.Time `json:"timestamp"`
	Error          string    `json:"error"`
	PieceCid       string    `json:"piece_cid"`
	ProposalID     string    `json:"deal_proposal_id"`
	ProposalCid    *string   `json:"deal_proposal_cid,omitempty"`
	TenantID       int16     `json:"tenant_id"`
	TenantClient   string    `json:"tenant_client_id"`
}
