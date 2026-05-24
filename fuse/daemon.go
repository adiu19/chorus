package fuse

import (
	"context"
	"syscall"

	fs "github.com/hanwen/go-fuse/v2/fs"
	fuse "github.com/hanwen/go-fuse/v2/fuse"
)

// the FUSE daemon is a per-step name resolver, not a path resolver

// ContentNode is a FUSE-based FS
type ContentNode struct {
	fs.Inode
}

// compile-time assertions
var _ = (fs.InodeEmbedder)((*ContentNode)(nil))
var _ = (fs.NodeLookuper)((*ContentNode)(nil))

func (c *ContentNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	ops := ContentNode{}
	out.Mode = 0644
	out.Size = 42
	return c.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFREG}), 0
}
