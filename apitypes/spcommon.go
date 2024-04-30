package apitypes

type TenantReplicationState struct {
	TenantID     int16   `json:"tenant_id"`
	TenantClient *string `json:"tenant_client_id"`

	MaxInFlightBytes int64 `json:"tenant_max_in_flight_bytes"`
	SpInFlightBytes  int64 `json:"actual_in_flight_bytes" db:"cur_in_flight_bytes"`

	MaxTotal     int16 `json:"tenant_max_total"`
	MaxOrg       int16 `json:"tenant_max_per_org"         db:"max_per_org"`
	MaxCity      int16 `json:"tenant_max_per_metro"       db:"max_per_city"`
	MaxCountry   int16 `json:"tenant_max_per_country"     db:"max_per_country"`
	MaxContinent int16 `json:"tenant_max_per_continent"   db:"max_per_continent"`

	Total       int16 `json:"actual_total"                db:"cur_total"`
	InOrg       int16 `json:"actual_within_org"           db:"cur_in_org"`
	InCity      int16 `json:"actual_within_metro"         db:"cur_in_city"`
	InCountry   int16 `json:"actual_within_country"       db:"cur_in_country"`
	InContinent int16 `json:"actual_within_continent"     db:"cur_in_continent"`

	DealAlreadyExists bool `json:"sp_holds_qualifying_deal"`
}

type SPInfo struct {
	Errors             []string            `json:"errors,omitempty"`
	SectorLog2Size     uint8               `json:"sector_log2_size"`
	PeerID             *string             `json:"peerid"`
	MultiAddrs         []string            `json:"multiaddrs"`
	RetrievalProtocols map[string][]string `json:"retrieval_protocols,omitempty"`
	PeerInfo           *struct {
		Protos map[string]struct{}    `json:"libp2p_protocols"`
		Meta   map[string]interface{} `json:"meta"`
	} `json:"peer_info,omitempty"`
}
