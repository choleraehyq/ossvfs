// Copyright 2015 Ka-Hing Cheung
// Copyright 2015 Google Inc. All Rights Reserved.
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
	"os"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	"github.com/denverdino/aliyungo/oss"
)

// Set up custom help text for goofys; in particular the usage section.
func init() {
	cli.AppHelpTemplate = `NAME:
   {{.Name}} - {{.Usage}}

USAGE:
   {{.Name}} {{if .Flags}}[global options]{{end}} bucket mountpoint
   {{if .Version}}
VERSION:
   {{.Version}}
   {{end}}{{if len .Authors}}
AUTHOR(S):
   {{range .Authors}}{{ . }}{{end}}
   {{end}}{{if .Commands}}
COMMANDS:
   {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
   {{end}}{{end}}{{if .Flags}}
GLOBAL OPTIONS:
   {{range .Flags}}{{.}}
   {{end}}{{end}}{{if .Copyright }}
COPYRIGHT:
   {{.Copyright}}
   {{end}}
`
}

func NewApp() (app *cli.App) {
	uid, gid := MyUserAndGroup()

	app = &cli.App{
		Name:     "ossvfs",
		Version:  "0.0.1",
		Usage:    "Mount an Aliyun OSS bucket locally",
		HideHelp: true,
		Writer:   os.Stderr,
		Flags: []cli.Flag{

			cli.BoolFlag{
				Name:  "help, h",
				Usage: "Print this help text and exit successfuly.",
			},

			/////////////////////////
			// File system
			/////////////////////////

			cli.StringSliceFlag{
				Name:  "o",
				Usage: "Additional system-specific mount options. Be careful!",
			},

			cli.IntFlag{
				Name:  "dir-mode",
				Value: 0755,
				Usage: "Permissions bits for directories. (default: 0755)",
			},

			cli.IntFlag{
				Name:  "file-mode",
				Value: 0644,
				Usage: "Permission bits for files (default: 0644)",
			},

			cli.IntFlag{
				Name:  "uid",
				Value: uid,
				Usage: "UID owner of all inodes.",
			},

			cli.IntFlag{
				Name:  "gid",
				Value: gid,
				Usage: "GID owner of all inodes.",
			},

			/////////////////////////
			// OSS
			/////////////////////////

			cli.StringFlag{
				Name:  "endpoint",
				Value: "",
				Usage: "The non-OSS endpoint to connect to." +
					" Possible values: http://127.0.0.1:8081/",
			},

			cli.BoolFlag{
				Name: "use-path-request",
				Usage: "Use a path-style request instead of virtual host-style." +
					" Needed for some private object stores.",
			},

			cli.BoolFlag{
				Name: "internal",
				Value: true,
				Usage: "Whether the machine is an ECS instance.",
			},

			/////////////////////////
			// Tuning
			/////////////////////////

			cli.DurationFlag{
				Name:  "stat-cache-ttl",
				Value: time.Minute,
				Usage: "How long to cache StatObject results and inode attributes.",
			},

			cli.DurationFlag{
				Name:  "type-cache-ttl",
				Value: time.Minute,
				Usage: "How long to cache name -> file/dir mappings in directory " +
					"inodes.",
			},

			/////////////////////////
			// Debugging
			/////////////////////////

			cli.BoolFlag{
				Name:  "debug_fuse",
				Usage: "Enable fuse-related debugging output.",
			},

			cli.BoolFlag{
				Name:  "debug_oss",
				Usage: "Enable OSS-related debugging output.",
			},
		},
	}

	return
}

type FlagStorage struct {
	// File system
	MountOptions map[string]string
	DirMode      os.FileMode
	FileMode     os.FileMode
	Uid          uint32
	Gid          uint32

	// OSS
	Endpoint       	string
	UsePathRequest 	bool
	Internal 	   	bool
	Region 		   	oss.Region
	AccessKeyId    	string
	AccessKeySecret string

	// Tuning
	StatCacheTTL time.Duration
	TypeCacheTTL time.Duration

	// Debugging
	DebugFuse bool
	DebugOSS  bool
}

func parseOptions(m map[string]string, s string) {
	// NOTE(jacobsa): The man pages don't define how escaping works, and as far
	// as I can tell there is no way to properly escape or quote a comma in the
	// options list for an fstab entry. So put our fingers in our ears and hope
	// that nobody needs a comma.
	for _, p := range strings.Split(s, ",") {
		var name string
		var value string

		// Split on the first equals sign.
		if equalsIndex := strings.IndexByte(p, '='); equalsIndex != -1 {
			name = p[:equalsIndex]
			value = p[equalsIndex+1:]
		} else {
			name = p
		}

		m[name] = value
	}

	return
}

// Add the flags accepted by run to the supplied flag set, returning the
// variables into which the flags will parse.
func PopulateFlags(c *cli.Context) (flags *FlagStorage) {
	flags = &FlagStorage{
		// File system
		MountOptions: make(map[string]string),
		DirMode:      os.FileMode(c.Int("dir-mode")),
		FileMode:     os.FileMode(c.Int("file-mode")),
		Uid:          uint32(c.Int("uid")),
		Gid:          uint32(c.Int("gid")),

		// Tuning,
		StatCacheTTL: c.Duration("stat-cache-ttl"),
		TypeCacheTTL: c.Duration("type-cache-ttl"),

		// OSS
		Endpoint:       c.String("endpoint"),
		UsePathRequest: c.Bool("use-path-request"),
		Internal: 		c.Bool("internal")

		// Debugging,
		DebugFuse: c.Bool("debug_fuse"),
		DebugOSS:  c.Bool("debug_oss"),
	}

	// Handle the repeated "-o" flag.
	for _, o := range c.StringSlice("o") {
		parseOptions(flags.MountOptions, o)
	}

	// Get the region/AccessKeyId/AccessKeySecret
	flags.AccessKeyId = os.Getenv("ACCESS_KEY_ID")
	flags.AccessKeySecret = os.Getenv("ACCESS_KEY_SECRET")
	flags.Region = oss.Region(os.Getenv("OSS_REGION"))
	
	return
}
