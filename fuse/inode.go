package fuse

import (
	"context"
	"encoding/hex"
	"syscall"

	"github.com/chorus/fuse/backend"
	fs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// the FUSE daemon is a per-step name resolver, not a path resolver

// rootNode is the directory at mount point
type rootNode struct {
	fs.Inode
	backend backend.Backend
}

// chunkNode represents a single chunk file. Implements Getattr (for stat)
// and Read (for the actual bytes).
type chunkNode struct {
	fs.Inode
	backend backend.Backend
	hash    []byte
}

// compile-time assertions
var _ = (fs.InodeEmbedder)((*rootNode)(nil))
var _ = (fs.NodeLookuper)((*rootNode)(nil))

var _ = (fs.InodeEmbedder)((*chunkNode)(nil))
var _ = (fs.NodeGetattrer)((*chunkNode)(nil))
var _ = (fs.NodeReader)((*chunkNode)(nil))

func (r *rootNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	hash, err := hex.DecodeString(name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	info, err := r.backend.Stat(hash)
	if err != nil {
		return nil, syscall.ENOENT
	}

	out.Size = uint64(info.Size)
	out.Mode = 0644
	child := &chunkNode{backend: r.backend, hash: hash}
	return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0

}

func (c *chunkNode) Getattr(ctx context.Context, fh fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	info, err := c.backend.Stat(c.hash)
	if err != nil {
		return syscall.ENOENT
	}
	out.Size = uint64(info.Size)
	out.Mode = 0644
	return 0
}

func (c *chunkNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	data, err := c.backend.Fetch(c.hash)
	if err != nil {
		return nil, syscall.EIO
	}
	if off >= int64(len(data)) {
		return gofuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return gofuse.ReadResultData(data[off:end]), 0
}
