//go:build tools
// +build tools

package tooling

import (
	_ "github.com/hannahhoward/cbor-gen-for"
	_ "golang.org/x/tools/cmd/stringer"
)
