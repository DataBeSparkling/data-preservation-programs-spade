package apitypes

//go:generate go run golang.org/x/tools/cmd/stringer -type=APIErrorCode -output=apierrors_gen.go

type APIErrorCode int

const (
	// Common
	ErrInvalidRequest            APIErrorCode = 4400
	ErrUnauthorizedAccess        APIErrorCode = 4401
	ErrSystemTemporarilyDisabled APIErrorCode = 4503

	// SP Reservation specific
	ErrOversizedPiece                  APIErrorCode = 4011
	ErrStorageProviderSuspended        APIErrorCode = 4012
	ErrStorageProviderIneligibleToMine APIErrorCode = 4013

	ErrStorageProviderInfoTooOld  APIErrorCode = 4041
	ErrStorageProviderUndialable  APIErrorCode = 4042
	ErrStorageProviderUnsupported APIErrorCode = 4043

	ErrUnclaimedPieceCID         APIErrorCode = 4020
	ErrProviderHasReplica        APIErrorCode = 4021
	ErrTenantsOutOfDatacap       APIErrorCode = 4022
	ErrTooManyReplicas           APIErrorCode = 4023
	ErrProviderAboveMaxInFlight  APIErrorCode = 4024
	ErrReplicationRulesViolation APIErrorCode = 4029 // catch-all for when there is no common rejection theme for competing tenants

	ErrExternalReservationRefused APIErrorCode = 4030 // some tenants are looking to add an additional check on their end
)
