// Package listen holds the shared modification-listener machinery used by
// the persistent map implementations (btree, htree): copy-on-write
// listener lists, listener identity semantics, and the error-joining fire
// path — all ports of Java's CopyOnWriteArrayList listener discipline and
// the "first throwable rethrown, remainder suppressed" delivery rule.
package listen

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// Push appends l to the immutable listener slice published at p (Java
// CopyOnWriteArrayList.addIfAbsent — same-identity listeners dedup).
// Registration order is the fire order. Caller serializes with *mu.
func Push[K, V any](mu *sync.Mutex, p *atomic.Pointer[[]mapdb5.MapModificationListener[K, V]], l mapdb5.MapModificationListener[K, V]) {
	mu.Lock()
	defer mu.Unlock()
	var cur []mapdb5.MapModificationListener[K, V]
	if old := p.Load(); old != nil {
		cur = *old
	}
	for _, x := range cur {
		if Same(x, l) {
			return
		}
	}
	n := make([]mapdb5.MapModificationListener[K, V], len(cur)+1)
	copy(n, cur)
	n[len(cur)] = l
	p.Store(&n)
}

// Remove drops every same-identity listener from p (Java COWAL.remove).
func Remove[K, V any](mu *sync.Mutex, p *atomic.Pointer[[]mapdb5.MapModificationListener[K, V]], l mapdb5.MapModificationListener[K, V]) {
	mu.Lock()
	defer mu.Unlock()
	old := p.Load()
	if old == nil {
		return
	}
	cur := *old
	out := make([]mapdb5.MapModificationListener[K, V], 0, len(cur))
	for _, x := range cur {
		if !Same(x, l) {
			out = append(out, x)
		}
	}
	p.Store(&out)
}

// CheckIdentity enforces the supported listener-identity domain before a
// registration is published. reflect.Value.Comparable reports whether
// interface equality CANNOT panic — including a comparable-declared struct
// holding an uncomparable dynamic value through an interface field.
// Non-comparable pointer-family values (Ptr/Chan/Map/Func/UnsafePointer)
// dedup by Pointer(). Anything else could register but could never be
// deduped or removed — reject it up front.
func CheckIdentity[K, V any](l mapdb5.MapModificationListener[K, V]) {
	v := reflect.ValueOf(l)
	if v.Comparable() {
		return
	}
	switch v.Kind() {
	case reflect.Ptr, reflect.Chan, reflect.Map, reflect.Func, reflect.UnsafePointer:
		return
	}
	panic(mapdb5.New(mapdb5.KindWrongConfiguration,
		"modification listener of type "+v.Type().String()+
			" has no stable identity: pass a pointer or a comparable value"))
}

// Same is listener identity for dedup/removal — Go's stand-in for Java
// Object.equals (identity by default): same dynamic type AND (equal
// comparable value, or equal pointer). Func-typed listeners dedup by code
// pointer only — two distinct closures of the same literal can alias, so
// listeners whose identity matters should be pointer types.
func Same[K, V any](a, b mapdb5.MapModificationListener[K, V]) bool {
	ta, tb := reflect.TypeOf(a), reflect.TypeOf(b)
	if ta != tb {
		return false
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	// Value-level comparability (NOT Type.Comparable): a struct declared
	// comparable can still hold an uncomparable dynamic value through an
	// interface field, and == would panic.
	if va.Comparable() && vb.Comparable() {
		return a == b
	}
	switch va.Kind() {
	case reflect.Ptr, reflect.Chan, reflect.Map, reflect.UnsafePointer, reflect.Func:
		return va.Pointer() == vb.Pointer()
	default:
		return false
	}
}

// Load snapshots the published listener slice (nil when none).
func Load[K, V any](p *atomic.Pointer[[]mapdb5.MapModificationListener[K, V]]) []mapdb5.MapModificationListener[K, V] {
	if ls := p.Load(); ls != nil {
		return *ls
	}
	return nil
}

// Fire invokes every listener, continuing past failures: every listener
// sees the event even when an earlier one fails; the first failure is
// returned (the rest joined — Java "first throwable rethrown, remainder
// suppressed"). The caller surfaces the returned error at its own ordering
// boundary (leaf lock released, split propagation completed, ...).
func Fire[K, V any](ls []mapdb5.MapModificationListener[K, V], key K, oldV, newV *V, triggered bool) error {
	var errs []error
	for _, l := range ls {
		// Per-invocation copies: the caller hands out pointers into its own
		// locals (the operation's captured old value AND its return value).
		// Each listener gets fresh copies — writing through the pointer can
		// change neither the values later listeners see nor the value the
		// operation returns. Absence stays nil.
		var o, n *V
		if oldV != nil {
			v := *oldV
			o = &v
		}
		if newV != nil {
			v := *newV
			n = &v
		}
		if err := Invoke(l, key, o, n, triggered); err != nil {
			errs = append(errs, err)
		}
	}
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return errors.Join(errs...)
	}
}

// Invoke runs one listener, converting a panic into a listener error (Java
// catches RuntimeException|Error) so the failure flows through the
// captured-error recovery path instead of unwinding through it.
func Invoke[K, V any](l mapdb5.MapModificationListener[K, V], key K, oldV, newV *V, triggered bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicAsError(r)
		}
	}()
	return l.Modify(key, oldV, newV, triggered)
}

// PanicAsError converts a captured panic value to an error, preserving a
// payload message so a listener panic still names itself (Java rethrows
// the original throwable).
func PanicAsError(r any) error {
	switch e := r.(type) {
	case nil:
		return nil
	case error:
		return e
	default:
		return fmt.Errorf("modification listener panicked: %v", r)
	}
}

// JoinSuppressed builds the panic value for a primary failure with
// secondary failures attached (Java Throwable.addSuppressed): the primary
// panic value verbatim when there are no secondaries, otherwise an
// errors.Join chain with the primary first — errors.Is/As still reach
// every member.
func JoinSuppressed(primary any, secondary ...any) any {
	var errs []error
	if e := PanicAsError(primary); e != nil {
		errs = append(errs, e)
	}
	for _, s := range secondary {
		if e := PanicAsError(s); e != nil {
			errs = append(errs, e)
		}
	}
	if len(errs) <= 1 {
		return primary
	}
	return errors.Join(errs...)
}
