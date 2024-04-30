// Package apitypes contains complete, dependency-free definitions of all
// possible responses of the ♠️ API. This includes the "root" ResponseEnvelope
// struct and all of its dependencies.
package apitypes

import (
	"time"
)

// ResponseEnvelope is the structure wrapping all responses from the deal engine
type ResponseEnvelope struct {
	RequestID          string          `json:"request_id,omitempty"`
	ResponseTime       time.Time       `json:"response_timestamp"`
	ResponseStateEpoch int64           `json:"response_state_epoch,omitempty"`
	ResponseCode       int             `json:"response_code"`
	ErrCode            int             `json:"error_code,omitempty"`
	ErrSlug            string          `json:"error_slug,omitempty"`
	ErrLines           []string        `json:"error_lines,omitempty"`
	InfoLines          []string        `json:"info_lines,omitempty"`
	ResponseEntries    *int            `json:"response_entries,omitempty"`
	Response           ResponsePayload `json:"response"`
}
type isResponsePayload struct{}

type ResponsePayload interface {
	is() isResponsePayload
}
