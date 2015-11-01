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
	"bytes"
	"fmt"
	"io"
	"io/ioutils"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/denverdino/aliyungo/oss"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/Sirupsen/logrus"
)

type Inode struct {
	Id         fuseops.InodeID
	Name       *string
	FullName   *string
	flags      *FlagStorage
	Attributes *fuseops.InodeAttributes

	log *logHandle

	mu      sync.Mutex          // everything below is protected by mu
	handles map[*DirHandle]bool // value is ignored
	refcnt  uint64
}

func NewInode(name *string, fullName *string, flags *FlagStorage) (inode *Inode) {
	inode = &Inode{Name: name, FullName: fullName, flags: flags}
	inode.handles = make(map[*DirHandle]bool)
	inode.refcnt = 1
	inode.log = GetLogger(*fullName)

	if inode.flags.DebugFuse {
		inode.log.Level = logrus.DebugLevel
	}
	return
}

func (inode *Inode) Ref() {
	inode.mu.Lock()
	defer inode.mu.Unlock()
	inode.refcnt++
}

type DirHandle struct {
	inode *Inode

	mu          sync.Mutex // everything below is protected by mu
	Entries     []fuseutil.Dirent
	NameToEntry map[string]fuseops.InodeAttributes // XXX use a smaller struct
	Marker      *string
	BaseOffset  int
}

func NewDirHandle(inode *Inode) (dh *DirHandle) {
	dh = &DirHandle{inode: inode}
	dh.NameToEntry = make(map[string]fuseops.InodeAttributes)
	return
}

type FileHandle struct {
	inode *Inode

	dirty     bool
	writeInit sync.Once
	mpuWG     sync.WaitGroup
	etags     []*string
	size      []int64

	mu              sync.Mutex
	mpu             *oss.Multi
	nextWriteOffset int64
	lastPartId      int

	poolHandle *BufferPoolHandle
	buf        []byte

	lastWriteError error

	// read
	reader        io.ReadCloser
	readBufOffset int64
}

func NewFileHandle(in *Inode) *FileHandle {
	fh := &FileHandle{inode: in}
	return fh
}

func (inode *Inode) logFuse(op string, args ...interface{}) {
	fuseLog.Debugln(op, inode.Id, *inode.FullName, args)
}

// LOCKS_REQUIRED(parent.mu)
func (parent *Inode) lookupFromDirHandles(name string) (inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	defer func() {
		if inode != nil {
			inode.Ref()
		}
	}()

	for dh := range parent.handles {
		attr, ok := dh.NameToEntry[name]
		if ok {
			fullName := parent.getChildName(name)
			inode = NewInode(&name, &fullName, parent.flags)
			inode.Attributes = &attr
			return
		}
	}

	return
}

func (parent *Inode) LookUp(fs *Ossvfs, name string) (inode *Inode, err error) {
	parent.logFuse("Inode.LookUp", name)

	inode = parent.lookupFromDirHandles(name)
	if inode != nil {
		return
	}

	inode, err = fs.LookUpInodeMaybeDir(name, parent.getChildName(name))
	if err != nil {
		return nil, err
	}

	return
}

func (parent *Inode) getChildName(name string) string {
	if parent.Id == fuseops.RootInodeID {
		return name
	} else {
		return fmt.Sprintf("%v/%v", *parent.FullName, name)
	}
}

func (inode *Inode) DeRef(n uint64) (stale bool) {
	inode.logFuse("ForgetInode", n)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	if inode.refcnt < n {
		panic(fmt.Sprintf("deref %v from %v", n, inode.refcnt))
	}

	inode.refcnt -= n

	return inode.refcnt == 0
}

func (parent *Inode) Unlink(fs *Ossvfs, name string) (err error) {
	parent.logFuse("Unlink", name)

	fullName := parent.getChildName(name)

	err := fs.bucket.Del(fullName)
	if err != nil {
		return mapOssError(err)
	}

	return
}

func (parent *Inode) Create(
	fs *Ossvfs,
	name string) (inode *Inode, fh *FileHandle) {

	parent.logFuse("Create", name)
	fullName := parent.getChildName(name)

	parent.mu.Lock()
	defer parent.mu.Unlock()

	now := time.Now()
	inode = NewInode(&name, &fullName, parent.flags)
	inode.Attributes = &fuseops.InodeAttributes{
		Size:   0,
		Nlink:  1,
		Mode:   fs.flags.FileMode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fs.flags.Uid,
		Gid:    fs.flags.Gid,
	}

	fh = NewFileHandle(inode)
	fh.poolHandle = fs.bufferPool.NewPoolHandle()
	fh.dirty = true

	return
}

func (parent *Inode) MkDir(
	fs *Ossvfs,
	name string) (inode *Inode, err error) {

	parent.logFuse("MkDir", name)

	fullName := parent.getChildName(name) + "/"

	// use a empty file to represent a dir
	_, err = fs.bucket.Put(fullName, []byte{}, "content-type",
		oss.Private, oss.Options{})
	if err != nil {
		err = mapOssError(err)
		return
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode = NewInode(&name, &fullName, parent.flags)
	inode.Attributes = &fs.rootAttrs

	return
}

func isEmptyDir(fs *Ossvfs, fullName string) (isDir bool, err error) {
	fullName += "/"

	resp, err := fs.bucket.List(fullName, "/", "", 2)
	if err != nil {
		return false, mapOssError(err)
	}

	if len(resp.CommonPrefixes) > 0 || len(resp.Contents) > 1 {
		err = fuse.ENOTEMPTY
		isDir = true
		return
	}

	if len(resp.Contents) == 1 {
		isDir = true

		if *resp.Contents[0].Key != fullName {
			err = fuse.ENOTEMPTY
		}
	}

	return
}

func (parent *Inode) RmDir(
	fs *Ossvfs,
	name string) (err error) {

	parent.logFuse("Rmdir", name)

	fullName := parent.getChildName(name)

	isDir, err := isEmptyDir(fs, fullName)
	if err != nil {
		return
	}
	if !isDir {
		return fuse.ENOENT
	}

	fullName += "/"

	_, err = fs.bucket.Del(fullName)
	if err != nil {
		return mapOssError(err)
	}

	return
}

func (inode *Inode) GetAttributes(fs *Ossvfs) (*fuseops.InodeAttributes, error) {
	// XXX refresh attributes
	inode.logFuse("GetAttributes")
	return inode.Attributes, nil
}

func (inode *Inode) OpenFile(fs *Ossvfs) *FileHandle {
	inode.logFuse("OpenFile")
	return NewFileHandle(inode)
}

func (fh *FileHandle) initWrite(fs *Ossvfs) {
	fh.writeInit.Do(func() {
		fh.mpuWG.Add(1)
		go fh.initMPU(fs)
	})
}

func (fh *FileHandle) initMPU(fs *Ossvfs) {
	defer func() {
		fh.mpuWG.Done()
	}()

	resp, err := fs.bucket.InitMulti(fh.inode.FullName, "",
		oss.Private, oss.Options{})

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if err != nil {
		fh.lastWriteError = mapOssError(err)
	}

	ossLog.Debug(resp)

	fh.mpu = resp
	fh.etags = make([]*string, 10000) // at most 10K parts
	fh.size = make([]int64, 10000)

	return
}

func (fh *FileHandle) mpuPartNoSpawn(buf []byte, part int) (err error) {
	fh.inode.logFuse("mpuPartNoSpawn", cap(buf), part)
	if cap(buf) != 0 {
		defer fh.poolHandle.Free(buf)
	}

	if part == 0 || part > 10000 {
		panic(fmt.Sprintf("invalid part number: %v", part))
	}

	resp, err := fh.mpu.PutPart(n, bytes.NewReader(buf))
	if err != nil {
		return mapOssError(err)
	}

	en := &fh.etags[part-1]

	if *en != nil {
		panic(fmt.Sprintf("etags for part %v already set: %v", part, **en))
	}
	*en = resp.ETag

	if fh.size[part-1] != 0 {
		panic(fmt.Sprintf("size for part %v already set: %v",
			part, fh.size[part-1]))
	}
	fh.size[part-1] = resp.Size

	return
}

func (fh *FileHandle) mpuPart(buf []byte, part int) {
	defer func() {
		fh.mpuWG.Done()
	}()

	// maybe wait for CreateMultipartUpload
	if fh.mpu == nil {
		fh.mpuWG.Wait()
		// initMPU might have errored
		if fh.mpu == nil {
			return
		}
	}

	err := fh.mpuPartNoSpawn(buf, part)
	if err != nil {
		fh.mu.Lock()
		defer fh.mu.Unlock()

		if fh.lastWriteError == nil {
			fh.lastWriteError = mapOssError(err)
		}
	}
}

func (fh *FileHandle) waitForCreateMPU(fs *Ossvfs) (err error) {
	if fh.mpu == nil {
		fh.mu.Unlock()
		fh.initWrite(fs)
		fh.mpuWG.Wait() // wait for initMPU
		fh.mu.Lock()

		if fh.lastWriteError != nil {
			return fh.lastWriteError
		}
	}

	return
}

func (fh *FileHandle) WriteFile(fs *Ossvfs, offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if offset != fh.nextWriteOffset {
		fh.inode.logFuse("WriteFile: only sequential writes supported", fh.nextWriteOffset, offset)
		fh.lastWriteError = fuse.EINVAL
		return fh.lastWriteError
	}

	if offset == 0 {
		fh.poolHandle = fs.bufferPool.NewPoolHandle()
		fh.dirty = true
	}

	for {
		if cap(fh.buf) == 0 {
			fh.buf = fh.poolHandle.Request()
		}

		nCopied := fh.poolHandle.Copy(&fh.buf, data)
		fh.nextWriteOffset += int64(nCopied)

		if len(fh.buf) == cap(fh.buf) {
			// we filled this buffer, upload this part
			err = fh.waitForCreateMPU(fs)
			if err != nil {
				return
			}

			fh.lastPartId++
			part := fh.lastPartId
			buf := fh.buf
			fh.buf = nil
			fh.mpuWG.Add(1)

			go fh.mpuPart(buf, part)
		}

		if nCopied == len(data) {
			break
		}

		data = data[nCopied:]
	}

	fh.inode.Attributes.Size = uint64(offset + int64(len(data)))

	return
}

// TODO: Remove this function.
// The returned value of s3.GetObject only contains a io.ReadCloser,
// so it need a middle layer to transfer to []byte.
// But oss.bucket.Get directly return a []byte.
// For minimize the changes, I create a io.ReadCloser to fit the current code.
// Remove tryReadAll, readFromStream and fh.reader when refactor.
func tryReadAll(r io.ReadCloser, buf []byte) (bytesRead int, err error) {
	toRead := len(buf)
	for toRead > 0 {
		buf := buf[bytesRead : bytesRead+int(toRead)]

		nread, err := r.Read(buf)
		bytesRead += nread
		toRead -= nread

		if err != nil {
			return bytesRead, err
		}
	}

	return
}

func (fh *FileHandle) readFromStream(offset int64, buf []byte) (bytesRead int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.inode.flags.DebugFuse {
		defer func() {
			fh.inode.logFuse("< readFromStream", bytesRead)
		}()
	}

	if fh.reader != nil {
		// try to service read from existing stream
		if offset == fh.readBufOffset {
			bytesRead, err = tryReadAll(fh.reader, buf)
			if err == io.EOF {
				fh.reader.Close()
				fh.reader = nil
			}
			fh.readBufOffset += int64(bytesRead)
			return
		} else {
			// XXX out of order read, maybe disable prefetching
			fh.inode.logFuse("out of order read", offset, fh.readBufOffset)
			fh.readBufOffset = offset
			if fh.reader != nil {
				fh.reader.Close()
				fh.reader = nil
			}
		}
	}

	return
}

func (fh *FileHandle) ReadFile(fs *Ossvfs, offset int64, buf []byte) (bytesRead int, err error) {
	fh.inode.logFuse("ReadFile", offset, len(buf), fh.readBufOffset)
	defer func() {
		if bytesRead != 0 && err != nil {
			err = nil
		}

		if fh.inode.flags.DebugFuse {
			fh.inode.logFuse("< ReadFile", bytesRead)
		}
	}()

	if uint64(offset) >= fh.inode.Attributes.Size {
		// nothing to read
		return
	}

	bytesRead, err = fh.readFromStream(offset, buf)
	if err != nil {
		return
	}

	if bytesRead == len(buf) || uint64(offset) == fh.inode.Attributes.Size {
		// nothing more to read
		return
	}

	offset += int64(bytesRead)
	buf = buf[bytesRead:]

	data, err := fs.bucket.Get(fh.inode.FullName)
	if err != nil {
		return bytesRead, mapOssError(err)
	}

	respReadCloser := ioutils.NopCloser(bytes.NewReader(data))
	fh.reader = respReadCloser

	nread, err := tryReadAll(respReadCloser, buf)
	if err == io.EOF {
		fh.reader.Close()
		fh.reader = nil
	}
	fh.readBufOffset += int64(nread)
	bytesRead += nread

	return
}

func (fh *FileHandle) flushSmallFile(fs *Ossvfs) (err error) {
	buf := fh.buf
	fh.buf = nil

	if cap(buf) != 0 {
		defer fh.poolHandle.Free(buf)
	}

	_, err = fs.bucket.Put(fh.inode.FullName, buf, "content-type",
		oss.Private, oss.Options{})
	if err != nil {
		err = mapOssError(err)
	}
	return
}

func (fh *FileHandle) FlushFile(fs *Ossvfs) (err error) {
	fh.inode.logFuse("FlushFile")

	if !fh.dirty {
		return
	}

	// abort mpu on error
	defer func() {
		if err != nil {
			fh.inode.logFuse("<-- FlushFile", err)
			if fh.mpu != nil {
				go func() {
					_ := fh.mpu.Abort()
					fh.mpu = nil
				}()
			}
		}

		fh.writeInit = sync.Once{}
		fh.nextWriteOffset = 0
		fh.lastPartId = 0
		fh.dirty = false
	}()

	if fh.lastPartId == 0 {
		return fh.flushSmallFile(fs)
	}

	fh.mpuWG.Wait()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if fh.mpu == nil {
		return
	}

	nParts := fh.lastPartId
	if fh.buf != nil {
		// upload last part
		nParts++
		err = fh.mpuPartNoSpawn(fh.buf, nParts)
		if err != nil {
			return
		}
	}

	parts := make([]oss.Part, nParts)
	for i := 0; i < nParts; i++ {
		parts[i] = oss.Part{
			N:    i + 1,
			ETag: fh.etags[i],
			Size: fh.size[i],
		}
	}

	err := fh.mpu.Complete(parts)
	if err != nil {
		return mapOssError(err)
	}

	fh.mpu = nil

	return
}

func (parent *Inode) Rename(fs *Ossvfs, from string, newParent *Inode, to string) (err error) {
	parent.logFuse("Rename", from, newParent.getChildName(to))

	fromFullName := parent.getChildName(from)

	// XXX don't hold the lock the entire time
	parent.mu.Lock()
	defer parent.mu.Unlock()

	fromIsDir, err := isEmptyDir(fs, fromFullName)
	if err != nil {
		// we don't support renaming a directory that's not empty
		return
	}

	toFullName := newParent.getChildName(to)

	if parent != newParent {
		newParent.mu.Lock()
		defer newParent.mu.Unlock()
	}

	toIsDir, err := isEmptyDir(fs, toFullName)
	if err != nil {
		return
	}

	if fromIsDir && !toIsDir {
		return fuse.ENOTDIR
	} else if !fromIsDir && toIsDir {
		return syscall.EISDIR
	}

	size := int64(-1)
	if fromIsDir {
		fromFullName += "/"
		toFullName += "/"
		size = 0
	}

	err = fs.copyObjectMaybeMultipart(size, fromFullName, toFullName)
	if err != nil {
		return err
	}

	_, err = fs.bucket.Del(fromFullName)
	if err != nil {
		return mapOssError(err)
	}

	return
}

func (inode *Inode) OpenDir() (dh *DirHandle) {
	inode.logFuse("OpenDir")

	dh = NewDirHandle(inode)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	inode.handles[dh] = true

	return
}

// Dirents, sorted by name.
type sortedDirents []fuseutil.Dirent

func (p sortedDirents) Len() int           { return len(p) }
func (p sortedDirents) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p sortedDirents) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func makeDirEntry(name string, t fuseutil.DirentType) fuseutil.Dirent {
	return fuseutil.Dirent{Name: name, Type: t, Inode: fuseops.RootInodeID + 1}
}

func (dh *DirHandle) ReadDir(fs *Ossvfs, offset fuseops.DirOffset) (*fuseutil.Dirent, error) {
	// If the request is for offset zero, we assume that either this is the first
	// call or rewinddir has been called. Reset state.
	if offset == 0 {
		dh.Entries = nil
	}

	if offset == 0 {
		e := makeDirEntry(".", fuseutil.DT_Directory)
		e.Offset = 1
		dh.NameToEntry["."] = fs.rootAttrs
		return &e, nil
	} else if offset == 1 {
		e := makeDirEntry("..", fuseutil.DT_Directory)
		e.Offset = 2
		dh.NameToEntry[".."] = fs.rootAttrs
		return &e, nil
	}

	i := int(offset) - dh.BaseOffset - 2
	if i < 0 {
		panic(fmt.Sprintf("invalid offset %v, base=%v", offset, dh.BaseOffset))
	}

	if i >= len(dh.Entries) {
		if dh.Marker != nil {
			dh.Entries = nil
			dh.BaseOffset += i
			i = 0
		}
	}

	if i > 5000 {
		// XXX prevent infinite loop, raise the limit later
		panic("too many results")
	}

	if dh.Entries == nil {
		prefix := *dh.inode.FullName
		if len(prefix) != 0 {
			prefix += "/"
		}

		resp, err := fs.bucket.List(prefix, "/", dh.Marker, 0)
		if err != nil {
			return nil, mapOssError(err)
		}

		ossLog.Debug(resp)

		dh.Entries = make([]fuseutil.Dirent, 0, len(resp.CommonPrefixes)+len(resp.Contents))

		for _, dir := range resp.CommonPrefixes {
			// strip trailing /
			dirName := dir[0 : len(dir)-1]
			// strip previous prefix
			dirName = dirName[len(prefix):]
			dh.Entries = append(dh.Entries, makeDirEntry(dirName, fuseutil.DT_Directory))
			dh.NameToEntry[dirName] = fs.rootAttrs
		}

		for _, obj := range resp.Contents {
			baseName := obj.Key[len(prefix):]
			if len(baseName) == 0 {
				// this is a directory blob
				continue
			}
			dh.Entries = append(dh.Entries, makeDirEntry(baseName, fuseutil.DT_File))
			dh.NameToEntry[baseName] = fuseops.InodeAttributes{
				Size:   uint64(obj.Size),
				Nlink:  1,
				Mode:   fs.flags.FileMode,
				Atime:  obj.LastModified,
				Mtime:  obj.LastModified,
				Ctime:  obj.LastModified,
				Crtime: obj.LastModified,
				Uid:    fs.flags.Uid,
				Gid:    fs.flags.Gid,
			}
		}

		sort.Sort(sortedDirents(dh.Entries))

		// Fix up offset fields.
		for i := 0; i < len(dh.Entries); i++ {
			en := &dh.Entries[i]
			// offset is 1 based, also need to account for "." and ".."
			en.Offset = fuseops.DirOffset(i+dh.BaseOffset) + 1 + 2
		}

		if *resp.IsTruncated {
			dh.Marker = resp.NextMarker
		} else {
			dh.Marker = nil
		}
	}

	if i == len(dh.Entries) {
		// we've reached the end
		return nil, nil
	} else if i > len(dh.Entries) {
		return nil, fuse.EINVAL
	}

	return &dh.Entries[i], nil
}

func (dh *DirHandle) CloseDir() error {
	inode := dh.inode

	inode.mu.Lock()
	defer inode.mu.Unlock()
	delete(inode.handles, dh)

	return nil
}
