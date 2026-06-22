package main

import "runtime/debug"

// version is injected at build time via:
//
//	-ldflags "-X main.version=$(git describe --tags --always --dirty)"
//
// (see the Makefile / Dockerfile). When unset, appVersion falls back to the VCS
// info Go embeds for an in-tree `go build`.
var version = "dev"

func appVersion() string {
	if version != "dev" {
		return version
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	rev, dirty := "", ""
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if len(s.Value) >= 12 {
				rev = s.Value[:12]
			} else {
				rev = s.Value
			}
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if rev != "" {
		return rev + dirty
	}
	return version
}
