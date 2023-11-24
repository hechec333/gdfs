package fs

import (
	"gdfs/internal/types"
	"gdfs/toolkit/cli"
	"io"
)

// gdfsctl get ${file}

func upload(ctx cli.CliContext, path types.Path, r io.Reader) (int64, error) {

}

// gdfsctl get ${file}
func download(ctx cli.CliContext, path types.Path, w io.Writer) (int64, error) {

}

// gdfsctl get ${file} --chunk ${id}
func downloadChunk(ctx cli.CliContext, path types.Path, id types.ChunkHandle, w io.Writer) (int64, error) {

}
