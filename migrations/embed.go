package migrations

import "embed"

// Files stores all numbered SQL migrations.
//
//go:embed *.sql
var Files embed.FS
