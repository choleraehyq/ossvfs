// Copyright 2015 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/denverdino/aliyungo/oss"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/Sirupsen/logrus"
)

// ossvfs is a Filey System written in Go. All the backend data is
// stored on Aliyun OSS as is. It's a Filey System instead of a File System
// because it makes minimal effort at being POSIX
// compliant. Particularly things that are difficult to support on OSS
// or would translate into more than one round-trip would either fail
// (rename non-empty dir) or faked (no per-file permission). ossvfs
// does not have a on disk data cache, and consistency model is
// close-to-open.

type Ossvfs struct {
	fuseutil.NotImplementedFileSystem
	bucket     *oss.Bucket
	bucketName string

	flags *FlagStorage

	umask uint32

	client    *oss.Client
	rootAttrs fuseops.InodeAttributes

	bufferPool *BufferPool

	// A lock protecting the state of the file system struct itself (distinct
	// from per-inode locks). Make sure to see the notes on lock ordering above.
	mu sync.Mutex

	// The next inode ID to hand out. We assume that this will never overflow,
	// since even if we were handing out inode IDs at 4 GHz, it would still take
	// over a century to do so.
	//
	// GUARDED_BY(mu)
	nextInodeID fuseops.InodeID

	// The collection of live inodes, keyed by inode ID. No ID less than
	// fuseops.RootInodeID is ever used.
	//
	// INVARIANT: For all keys k, fuseops.RootInodeID <= k < nextInodeID
	// INVARIANT: For all keys k, inodes[k].ID() == k
	// INVARIANT: inodes[fuseops.RootInodeID] is missing or of type inode.DirInode
	// INVARIANT: For all v, if IsDirName(v.Name()) then v is inode.DirInode
	//
	// GUARDED_BY(mu)
	inodes      map[fuseops.InodeID]*Inode
	inodesCache map[string]*Inode // fullname to inode

	nextHandleID fuseops.HandleID
	dirHandles   map[fuseops.HandleID]*DirHandle

	fileHandles map[fuseops.HandleID]*FileHandle
}

var ossLog = GetLogger("oss")

func NewOssvfs(bucket string, flags *FlagStorage) *Ossvfs {
	// Set up the basic struct.
	fs := &Ossvfs{
		bucketName: bucket,
		flags:      flags,
		umask:      0122,
	}

	fs.client = oss.NewOSSClient(flags.Region, flags.Internal,
		flags.AccessKeyId, flags.AccessKeySecret, true)

	fs.bucket = fs.client.Bucket(bucket)

	if flags.DebugOSS {
		fs.client.SetDebug(flags.DebugOSS)
		ossLog.Level = logrus.DebugLevel
	}

	location, err := fs.bucket.Location()
	if err != nil {
		if mapOssError(err) == fuse.ENOENT {
			log.Errorf("bucket %v does not exist", bucket)
			return nil
		}
	}

	if oss.Region(location) != fs.flags.Region {
		log.Errorf("the location of bucket %v is wrong")
		return nil
	}

	now := time.Now()
	fs.rootAttrs = fuseops.InodeAttributes{
		Size:   4096,
		Nlink:  2,
		Mode:   flags.DirMode | os.ModeDir,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fs.flags.Uid,
		Gid:    fs.flags.Gid,
	}

	fs.bufferPool = NewBufferPool(1000*1024*1024, 200*1024*1024)

	fs.nextInodeID = fuseops.RootInodeID + 1
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	root := NewInode(getStringPointer(""), getStringPointer(""), flags)
	root.Id = fuseops.RootInodeID
	root.Attributes = &fs.rootAttrs

	fs.inodes[fuseops.RootInodeID] = root
	fs.inodesCache = make(map[string]*Inode)

	fs.nextHandleID = 1
	fs.dirHandles = make(map[fuseops.HandleID]*DirHandle)

	fs.fileHandles = make(map[fuseops.HandleID]*FileHandle)

	return fs
}

// Find the given inode. Panic if it doesn't exist.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *Ossvfs) getInodeOrDie(id fuseops.InodeID) (inode *Inode) {
	inode = fs.inodes[id]
	if inode == nil {
		panic(fmt.Sprintf("Unknown inode: %v", id))
	}

	return
}

func (fs *Ossvfs) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {

	const BLOCK_SIZE = 4096
	const TOTAL_SPACE = 1 * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
	const TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE
	const INODES = 1 * 1000 * 1000 * 1000 // 1 billion
	op.BlockSize = BLOCK_SIZE
	op.Blocks = TOTAL_BLOCKS
	op.BlocksFree = TOTAL_BLOCKS
	op.BlocksAvailable = TOTAL_BLOCKS
	op.IoSize = 1 * 1024 * 1024 // 1MB
	op.Inodes = INODES
	op.InodesFree = INODES
	return
}

func (fs *Ossvfs) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {

	fs.mu.Lock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	attr, err := inode.GetAttributes(fs)
	op.Attributes = *attr
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return
}

func mapOssError(err error) error {
	if ossErr, ok := err.(*oss.Error); ok {
		switch ossErr.StatusCode {
		case 404:
			return fuse.ENOENT
		case 405:
			return syscall.ENOTSUP
		default:
			ossLog.Errorf("code=%v msg=%v request=%v\n", ossErr.Message, ossErr.StatusCode, ossErr.RequestId)
			return err
		}
	} else {
		return err
	}
}

func (fs *Ossvfs) LookUpInodeNotDir(name string, c chan *http.Response, errc chan error) {
	resp, err := fs.bucket.Head(name, nil)
	if err != nil {
		errc <- mapOssError(err)
		return
	}

	ossLog.Debug(resp)
	c <- resp
}

func (fs *Ossvfs) LookUpInodeDir(name string, c chan *oss.ListResp, errc chan error) {

	resp, err := fs.bucket.List(name+"/", "/", "", 1)
	if err != nil {
		errc <- mapOssError(err)
		return
	}

	ossLog.Debug(resp)
	c <- resp
}

func (fs *Ossvfs) copyObjectMaybeMultipart(size int64, from string, to string) (err error) {
	if size == -1 {
		size, err = fs.bucket.GetContentLength(from)
		if err != nil {
			return mapOssError(err)
		}
	}

	from = fs.bucket.Path(from)

	if size > 1*1024*1024*1024 {
		return fs.bucket.CopyLargeFile(from, to, "application/octet-stream",
			oss.Private, oss.Options{})
	}

	_, err = fs.bucket.PutCopy(to, oss.Private, oss.CopyOptions{}, from)
	if err != nil {
		err = mapOssError(err)
	}

	return
}

func (fs *Ossvfs) allocateInodeId() (id fuseops.InodeID) {
	id = fs.nextInodeID
	fs.nextInodeID++
	return
}

// returned inode has nil Id
func (fs *Ossvfs) LookUpInodeMaybeDir(name string, fullName string) (inode *Inode, err error) {
	errObjectChan := make(chan error, 1)
	objectChan := make(chan *http.Response, 1)
	errDirChan := make(chan error, 1)
	dirChan := make(chan *oss.ListResp, 1)

	go fs.LookUpInodeNotDir(fullName, objectChan, errObjectChan)
	go fs.LookUpInodeDir(fullName, dirChan, errDirChan)

	notFound := false

	for {
		select {
		// TODO: if both object and object/ exists, return dir
		case resp := <-objectChan:
			inode = NewInode(&name, &fullName, fs.flags)
			lastModifiedTime, tmperr := http.ParseTime(resp.Header.Get("Last-Modified"))
			if tmperr != nil {
				panic("Last " + resp.Header.Get("Last-Modified") + " modified time is invalid")
			}
			inode.Attributes = &fuseops.InodeAttributes{
				Size:   uint64(resp.ContentLength),
				Nlink:  1,
				Mode:   fs.flags.FileMode,
				Atime:  lastModifiedTime,
				Mtime:  lastModifiedTime,
				Ctime:  lastModifiedTime,
				Crtime: lastModifiedTime,
				Uid:    fs.flags.Uid,
				Gid:    fs.flags.Gid,
			}
			return
		case err = <-errObjectChan:
			if err == fuse.ENOENT {
				if notFound {
					return nil, err
				} else {
					notFound = true
					err = nil
				}
			} else {
				//TODO: retry
			}
		case resp := <-dirChan:
			if len(resp.CommonPrefixes) != 0 || len(resp.Contents) != 0 {
				inode = NewInode(&name, &fullName, fs.flags)
				inode.Attributes = &fs.rootAttrs
				return
			} else {
				// 404
				if notFound {
					return nil, fuse.ENOENT
				} else {
					notFound = true
				}
			}
		case err = <-errDirChan:
			//TODO: retry
		}
	}
}

func (fs *Ossvfs) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {

	fs.mu.Lock()

	parent := fs.getInodeOrDie(op.Parent)
	inode, ok := fs.inodesCache[parent.getChildName(op.Name)]
	if ok {
		defer inode.Ref()
	} else {
		fs.mu.Unlock()

		inode, err = parent.LookUp(fs, op.Name)
		if err != nil {
			return err
		}

		fs.mu.Lock()
		inode.Id = fs.allocateInodeId()
		fs.inodesCache[*inode.FullName] = inode
	}

	fs.inodes[inode.Id] = inode
	op.Entry.Child = inode.Id
	op.Entry.Attributes = *inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.TypeCacheTTL)
	fs.mu.Unlock()

	inode.logFuse("<-- LookUpInode")

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Ossvfs) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	fs.mu.Lock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	stale := inode.DeRef(op.N)

	if stale {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		delete(fs.inodes, op.Inode)
		delete(fs.inodesCache, *inode.FullName)
	}

	return
}

func (fs *Ossvfs) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	in := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	// XXX/is this a dir?
	dh := in.OpenDir()

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.dirHandles[handleID] = dh
	op.Handle = handleID

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Ossvfs) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {

	// Find the handle.
	fs.mu.Lock()
	dh := fs.dirHandles[op.Handle]
	//inode := fs.inodes[op.Inode]
	fs.mu.Unlock()

	if dh == nil {
		panic(fmt.Sprintf("can't find dh=%v", op.Handle))
	}

	dh.inode.logFuse("ReadDir", op.Offset)

	for i := op.Offset; ; i++ {
		e, err := dh.ReadDir(fs, i)
		if err != nil {
			return err
		}
		if e == nil {
			break
		}

		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], *e)
		if n == 0 {
			break
		}

		dh.inode.logFuse("<-- ReadDir", e.Name, e.Offset)

		op.BytesRead += n
	}

	return
}

func (fs *Ossvfs) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) (err error) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	dh := fs.dirHandles[op.Handle]
	dh.CloseDir()

	fuseLog.Debugln("ReleaseDirHandle", *dh.inode.FullName)

	delete(fs.dirHandles, op.Handle)

	return
}

func (fs *Ossvfs) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.Lock()
	in := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	fh := in.OpenFile(fs)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh

	op.Handle = handleID
	op.KeepPageCache = true

	return
}

func (fs *Ossvfs) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {

	fs.mu.Lock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.Unlock()

	op.BytesRead, err = fh.ReadFile(fs, op.Offset, op.Dst)

	return
}

func (fs *Ossvfs) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {

	fs.mu.Lock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.Unlock()

	err = fh.FlushFile(fs)
	return
}

func (fs *Ossvfs) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {

	fs.mu.Lock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.Unlock()

	err = fh.FlushFile(fs)

	return
}

func (fs *Ossvfs) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	delete(fs.fileHandles, op.Handle)
	return
}

func (fs *Ossvfs) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	inode, fh := parent.Create(fs, op.Name)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	nextInode := fs.nextInodeID
	fs.nextInodeID++

	inode.Id = nextInode

	fs.inodes[inode.Id] = inode
	fs.inodesCache[*inode.FullName] = inode

	op.Entry.Child = inode.Id
	op.Entry.Attributes = *inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.TypeCacheTTL)

	// Allocate a handle.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh

	op.Handle = handleID

	inode.logFuse("<-- CreateFile")

	return
}

func (fs *Ossvfs) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	// ignore op.Mode for now
	inode, err := parent.MkDir(fs, op.Name)
	if err != nil {
		return err
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	nextInode := fs.nextInodeID
	fs.nextInodeID++

	inode.Id = nextInode

	fs.inodes[inode.Id] = inode
	op.Entry.Child = inode.Id
	op.Entry.Attributes = *inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.TypeCacheTTL)

	return
}

func (fs *Ossvfs) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	err = parent.RmDir(fs, op.Name)
	return
}

func (fs *Ossvfs) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {
	// do nothing, we don't support any of the changes
	return
}

func (fs *Ossvfs) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {

	fs.mu.Lock()

	fh, ok := fs.fileHandles[op.Handle]
	if !ok {
		panic(fmt.Sprintf("WriteFile: can't find handle %v", op.Handle))
	}
	fs.mu.Unlock()

	err = fh.WriteFile(fs, op.Offset, op.Data)

	return
}

func (fs *Ossvfs) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	err = parent.Unlink(fs, op.Name)
	return
}

func (fs *Ossvfs) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.OldParent)
	newParent := fs.getInodeOrDie(op.NewParent)
	fs.mu.Unlock()

	return parent.Rename(fs, op.OldName, newParent, op.NewName)
}

func getStringPointer(v string) *string {
	return &v
}
