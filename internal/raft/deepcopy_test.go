package raft

import (
	"log/slog"
	"reflect"
	"testing"
	"unsafe"
)

// deepCopy returns a copy of the object graph under v that shares nothing
// mutable with it, aliasing preserved: two pointers to one object in the
// original point to one object in the copy. The exhaustive search copies a
// whole cluster (Raft instances, their logs, the simulated disks) instead
// of replaying its path. Funcs and loggers are shared: they hold no state
// a search step changes. Unexported fields are reached with unsafe, which
// is why this lives in a test file.
func deepCopy[T any](v T) T {
	c := copier{seen: map[uintptr]reflect.Value{}}
	src := reflect.ValueOf(&v).Elem()
	dst := reflect.New(src.Type()).Elem()
	c.copy(dst, src)
	return dst.Interface().(T)
}

type copier struct {
	seen map[uintptr]reflect.Value // original pointer -> its copy (a pointer value)
}

var loggerType = reflect.TypeFor[*slog.Logger]()

// settable makes a field of an addressable struct writable, exported or
// not.
func settable(v reflect.Value) reflect.Value {
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
}

func (c *copier) copy(dst, src reflect.Value) {
	switch src.Kind() {
	case reflect.Pointer:
		if src.IsNil() {
			return
		}
		if src.Type() == loggerType {
			dst.Set(src)
			return
		}
		if p, ok := c.seen[src.Pointer()]; ok {
			dst.Set(p)
			return
		}
		p := reflect.New(src.Type().Elem())
		c.seen[src.Pointer()] = p
		c.copy(p.Elem(), src.Elem())
		dst.Set(p)
	case reflect.Struct:
		for i := range src.NumField() {
			c.copy(settable(dst.Field(i)), settable(src.Field(i)))
		}
	case reflect.Slice:
		if src.IsNil() {
			return
		}
		// Slices that share a backing array in the original get separate
		// arrays here; no copied type relies on such sharing being kept.
		s := reflect.MakeSlice(src.Type(), src.Len(), src.Cap())
		for i := range src.Len() {
			c.copy(s.Index(i), src.Index(i))
		}
		dst.Set(s)
	case reflect.Array:
		for i := range src.Len() {
			c.copy(dst.Index(i), src.Index(i))
		}
	case reflect.Map:
		if src.IsNil() {
			return
		}
		m := reflect.MakeMapWithSize(src.Type(), src.Len())
		it := src.MapRange()
		for it.Next() {
			k := reflect.New(src.Type().Key()).Elem()
			c.copy(k, it.Key())
			val := reflect.New(src.Type().Elem()).Elem()
			c.copy(val, it.Value())
			m.SetMapIndex(k, val)
		}
		dst.Set(m)
	case reflect.Interface:
		if src.IsNil() {
			return
		}
		inner := reflect.New(src.Elem().Type()).Elem()
		c.copy(inner, src.Elem())
		dst.Set(inner)
	case reflect.Chan, reflect.UnsafePointer:
		panic("deepCopy: cannot copy " + src.Type().String())
	default: // values, and funcs (shared)
		dst.Set(src)
	}
}

// A copied cluster behaves as the original would, and changing one leaves
// the other alone.
func TestDeepCopyCluster(t *testing.T) {
	w, err := newWorld(nil)
	if err != nil {
		t.Fatal(err)
	}
	b := exploreBound{budget: budget{elections: 1, proposals: 1}}
	for range 6 { // an election and some messages
		if err := w.apply(w.enabled(b)[0]); err != nil {
			t.Fatal(err)
		}
	}
	before := w.fingerprint()
	cp := deepCopy(w)
	if cp.fingerprint() != before {
		t.Fatal("the copy differs from the original")
	}
	for range 6 {
		if err := cp.apply(cp.enabled(b)[0]); err != nil {
			t.Fatal(err)
		}
	}
	if w.fingerprint() != before {
		t.Fatal("changing the copy changed the original")
	}
	for range 6 {
		if err := w.apply(w.enabled(b)[0]); err != nil {
			t.Fatal(err)
		}
	}
	if w.fingerprint() != cp.fingerprint() {
		t.Fatal("the copy and the original diverged on the same events")
	}
}
