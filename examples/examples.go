// Package examples embeds the example workflow definitions, which the
// coordinator registers in the default namespace on startup.
package examples

import "embed"

//go:embed workflows/*.yaml
var Workflows embed.FS
