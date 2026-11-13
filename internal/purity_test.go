// Package internal has no code of its own. Its test enforces architecture
// rules 1 and 2 on every core package: see docs/adr/0003.
package internal

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const corePrefix = "github.com/amritangshudey/samsa/internal/"

// allowedImports is every standard-library package the core may use. None of
// them can reach the clock, the disk, the network, the scheduler or a random
// source, except time, which is restricted further by allowedTime.
// Adding to this list is an architecture decision, not a convenience.
var allowedImports = map[string]bool{
	"bytes":          true,
	"cmp":            true,
	"container/heap": true,
	// Deterministic hashing for SASL (ADR 0019); never crypto/rand, which
	// reads the operating system's randomness.
	"crypto/hmac":     true,
	"crypto/pbkdf2":   true,
	"crypto/sha256":   true,
	"crypto/sha512":   true,
	"crypto/subtle":   true,
	"encoding/base64": true,
	"hash":            true,
	"encoding/binary": true,
	"errors":          true,
	"fmt":             true,
	"hash/crc32":      true,
	"io":              true,
	"log/slog":        true,
	"math":            true,
	"math/bits":       true,
	// Parsing addresses and CIDR ranges for ACL hosts (ADR 0020); no I/O.
	"net/netip":    true,
	"path":         true,
	"slices":       true,
	"sort":         true,
	"strconv":      true,
	"strings":      true,
	"time":         true,
	"unicode/utf8": true,
}

// allowedTime is what the core may use from package time: the duration type
// and its units. Everything else reads or waits on the real clock.
var allowedTime = map[string]bool{
	"Duration":    true,
	"Nanosecond":  true,
	"Microsecond": true,
	"Millisecond": true,
	"Second":      true,
	"Minute":      true,
	"Hour":        true,
}

// violations returns one message per breach of the core purity rules in f.
func violations(fset *token.FileSet, f *ast.File) []string {
	var out []string
	report := func(pos token.Pos, format string, args ...any) {
		out = append(out, fmt.Sprintf("%s: %s", fset.Position(pos), fmt.Sprintf(format, args...)))
	}

	timeName := ""
	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		core := strings.HasPrefix(path, corePrefix) && !strings.HasPrefix(path, corePrefix+"realsys")
		if !allowedImports[path] && !core {
			report(imp.Pos(), "import %q is not allowed in the core", path)
		}
		// A dot import would let time.Now be written as Now, hiding it from the check below.
		if imp.Name != nil && imp.Name.Name == "." {
			report(imp.Pos(), "dot import of %q", path)
		}
		if path == "time" {
			timeName = "time"
			if imp.Name != nil {
				timeName = imp.Name.Name
			}
		}
	}

	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GoStmt:
			report(n.Pos(), "go statement: the core is single-threaded")
		case *ast.SelectStmt:
			report(n.Pos(), "select statement: the runtime picks the ready case at random")
		case *ast.SelectorExpr:
			if x, ok := n.X.(*ast.Ident); ok && timeName != "" && x.Name == timeName && !allowedTime[n.Sel.Name] {
				report(n.Pos(), "time.%s is not allowed in the core: use sys.Clock", n.Sel.Name)
			}
		}
		return true
	})
	return out
}

func TestCoreIsPure(t *testing.T) {
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "realsys" || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		for _, v := range violations(fset, f) {
			t.Error(v)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestViolations(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []string // one substring per expected violation, in order
	}{
		{"clean", `package p
import ("errors"; "time"; "github.com/amritangshudey/samsa/internal/sys")
var _ = errors.New
var _ sys.Instant
const d = 5 * time.Second
var _ time.Duration`, nil},
		{"os import", `package p
import "os"
var _ = os.Open`, []string{`import "os"`}},
		{"global rand", `package p
import "math/rand/v2"
var _ = rand.Uint64`, []string{`import "math/rand/v2"`}},
		{"context", `package p
import "context"
var _ context.Context`, []string{`import "context"`}},
		{"real implementations", `package p
import _ "github.com/amritangshudey/samsa/internal/realsys"`, []string{`internal/realsys"`}},
		{"third party", `package p
import _ "example.com/x"`, []string{`import "example.com/x"`}},
		{"time.Now", `package p
import "time"
var _ = time.Now()`, []string{"time.Now"}},
		{"time.Now via alias", `package p
import t "time"
var _ = t.Now()`, []string{"time.Now"}},
		{"time dot import", `package p
import . "time"
var _ = Now()`, []string{"dot import"}},
		{"timer", `package p
import "time"
func f() { <-time.After(time.Second) }`, []string{"time.After"}},
		{"goroutine", `package p
func f() { go f() }`, []string{"go statement"}},
		{"select", `package p
func f(a, b chan int) { select { case <-a: case <-b: } }`, []string{"select statement"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "p.go", tt.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			got := violations(fset, f)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d violations %q, want %d", len(got), got, len(tt.want))
			}
			for i, w := range tt.want {
				if !strings.Contains(got[i], w) {
					t.Errorf("violation %d = %q, want it to contain %q", i, got[i], w)
				}
			}
		})
	}
}
