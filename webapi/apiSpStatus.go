package main

import (
	"github.com/data-preservation-programs/spade/apitypes"
	"github.com/labstack/echo/v4"
)

func apiSpStatus(c echo.Context) error {
	_, ctxMeta := unpackAuthedEchoContext(c)

	return retFail(
		c,
		apitypes.ErrSystemTemporarilyDisabled,
		`
                                            !!! WE NEVER IMPLEMENTED THIS !!!

This area will contain various information regarding the system and the current state of Storage Provider %s
    `,
		ctxMeta.authedActorID.String(),
	)
}
