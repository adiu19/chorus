package fuse

import (
	"github.com/chorus/fuse/backend"
	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type Config struct {
	MountPoint string
	Backend    backend.Backend
	Debug      bool
}

func Mount(cfg Config) (*gofuse.Server, error) {
	root := &rootNode{backend: cfg.Backend}
	opts := &fs.Options{
		MountOptions: gofuse.MountOptions{
			Debug:      cfg.Debug,
			AllowOther: false,
			FsName:     "chorusfs",
			Name:       "chorus",
		},
	}

	return fs.Mount(cfg.MountPoint, root, opts)
}
