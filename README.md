Ossvfs is a Filey-System interface to [OSS](http://www.aliyun.com/product/oss/)

Ossvfs is reshaped from [Goofys](https://github.com/kahing/goofys). Thank the author of Goofys for his great work!

Ossvfs is still under heavy development and have *NOT* been fully tested. Do *NOT*
use it in production now.

# Overview

Ossvfs allows you to mount an OSS bucket as a filey system.

It's a Filey System instead of a File System because ossvfs strives
for performance first and POSIX second. Particularly things that are
difficult to support on OSS or would translate into more than one
round-trip would either fail (random writes) or faked (no per-file
permission). Ossvfs does not have a on disk data cache, and
consistency model is close-to-open.

# Usage

# Benchmark

# License

Licensed under the Apache License, Version 2.0

# Current Status

List of not yet implemented fuse operations:
  * in terms of syscalls
    * `readlink`
    * `chmod`/`utimes`/`ftruncate`
    * `fsync`

List of non-POSIX behaviors/limitations:
  * only sequential writes supported
  * does not support appending to a file yet
  * file mode is always 0644 for regular files and 0700 for directories
  * directories link count is always 2
  * file owner is always the user running goofys
  * `ctime`, `atime` is always the same as `mtime`
  * cannot rename non-empty directories
  * `unlink` returns success even if file is not present
  * can only create files up to 50GB
  * no `symlink` support

# References

  * Data is stored on [Aliyun OSS](http://www.aliyun.com/product/oss/)
  * [Aliyun SDK for Go](https://github.com/denverdino/aliyungo)
  * Other related fuse filesystems
    * [Goofys](https://github.com/kahing/goofys)
    * [s3fs](https://github.com/s3fs-fuse/s3fs-fuse): another popular filesystem for S3
    * [gcsfuse](https://github.com/googlecloudplatform/gcsfuse):
      filesystem for
      [Google Cloud Storage](https://cloud.google.com/storage/). Goofys
      borrowed some skeleton code from this project.
  * [S3Proxy](https://github.com/andrewgaul/s3proxy) is used for `go test`
  * [fuse binding](https://github.com/jacobsa/fuse), also used by `gcsfuse`
