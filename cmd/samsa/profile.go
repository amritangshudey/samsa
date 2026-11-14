package main

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Profiles and samsa doctor (docs/PRODUCTION.md, item 24). A profile is a set
// of defaults for the flags that neither the command line nor the config file
// set; `samsa doctor` prints every flag with its value, where the value came
// from, and what is risky about it.

var profiles = map[string]map[string]string{
	// dev: a laptop. Writes may sit in the operating system's cache.
	"dev": {"sync": "interval", "scrub-interval": "-1s"},
	// test: a CI job. Nothing is kept for long, nothing is checked at rest.
	"test": {"sync": "never", "log-level": "warn", "scrub-interval": "-1s", "producer-expiry": "10m"},
	// prod: what survives a power cut, and checks its own disk.
	"prod": {"sync": "always"},
}

// applyProfile sets the flags of the named profile that are not in set, and
// records where each flag's value came from in sources.
func applyProfile(fs *flag.FlagSet, name string, set map[string]string, clustered bool) error {
	if name == "" {
		return nil
	}
	defaults, ok := profiles[name]
	if !ok {
		return fmt.Errorf("-profile: %q is not dev, test or prod", name)
	}
	keys := make([]string, 0, len(defaults))
	for k := range defaults {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, given := set[k]; given {
			continue
		}
		if k == "sync" && clustered {
			continue // a cluster of several nodes needs -sync=always whatever the profile
		}
		if err := fs.Set(k, defaults[k]); err != nil {
			return fmt.Errorf("-profile %s: %s: %w", name, k, err)
		}
		set[k] = "profile " + name
	}
	return nil
}

// risk says what is risky about a flag's value, or "" if nothing is. A "!"
// at its start marks what a production node should not have.
func risk(name, value string, v map[string]string) string {
	switch name {
	case "sync":
		switch value {
		case "never":
			return "! acknowledged writes are lost when the machine loses power; a cluster refuses it"
		case "interval":
			return "! writes acknowledged within -sync-interval of a power cut are lost"
		}
	case "tls-cert":
		if value == "" {
			return "! client traffic is not encrypted"
		}
	case "sasl-mechanisms":
		if value == "" {
			return "! any client that can connect may read and write"
		}
	case "acl":
		if value == "false" {
			return "authorization is off: every authenticated client may do everything"
		}
	case "peers":
		if value == "" || !strings.Contains(value, ",") {
			return "! a single node: nothing to fail over to, nothing to repair damage from"
		}
	case "scrub-interval":
		if strings.HasPrefix(value, "-") {
			return "damaged data on disk is found only when something reads it"
		}
	case "log-level":
		if value == "debug" {
			return "logs every request: slow, and large"
		}
	case "faults":
		if value != "" {
			return "! fault injection is on: for testing applications, never for data that matters"
		}
	case "producer-expiry":
		if strings.HasPrefix(value, "-") {
			return "idempotent producer state is never forgotten: memory grows with every producer id"
		}
	case "retention-time":
		if value == "0s" && v["retention-bytes"] == "0" && v["retention-bytes-per-partition"] == "0" {
			return "nothing expires unless a topic sets retention.ms or retention.bytes: disk grows without bound"
		}
	}
	return ""
}

// doctor writes the report and says whether anything marked "!" is there.
func doctor(w io.Writer, fs *flag.FlagSet, set map[string]string, profile string) (danger bool) {
	values := map[string]string{}
	fs.VisitAll(func(f *flag.Flag) { values[f.Name] = f.Value.String() })
	var names []string
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	_, _ = fmt.Fprintf(w, "samsa doctor: profile %q\n\n", profile)
	var lines []string
	for _, name := range names {
		if name == "config" || name == "version" || name == "profile" {
			continue
		}
		source := "default"
		if s, ok := set[name]; ok {
			source = s
		}
		note := risk(name, values[name], values)
		if note == "" && source == "default" {
			continue // only what was chosen, and what is risky, is worth a line
		}
		if strings.HasPrefix(note, "!") {
			danger = true
		}
		lines = append(lines, fmt.Sprintf("  %-34s %-18s (%s)  %s", name, values[name], source, note))
	}
	if len(lines) == 0 {
		_, _ = fmt.Fprintln(w, "  nothing is set and nothing is risky")
	}
	for _, l := range lines {
		_, _ = fmt.Fprintln(w, strings.TrimRight(l, " "))
	}
	if danger {
		_, _ = fmt.Fprintln(w, "\nLines marked ! are risky for a node that keeps data that matters.")
	}
	return danger
}
