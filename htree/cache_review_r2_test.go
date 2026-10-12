package htree

import (
	"errors"
	"math"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/store"
)

func r2CheckCoherent(t *testing.T, m *HTreeCache[int64, int64]) {
	t.Helper()
	var physical, queued int64
	for seg, root := range m.inner.segmentRoots {
		TreeFold(root, m.inner.st, m.inner.levels-1, func(_, r int64) { physical += int64(len(m.loadLeaf(r).keys)) })
		queued += int64(m.inner.queues[seg].Len())
		m.inner.queues[seg].Verify()
	}
	if n := m.LenLong(); n != physical || n != queued {
		t.Fatalf("counter=%d physical=%d queue=%d", n, physical, queued)
	}
}

func TestCacheReviewR2ReopenClockOrder(t *testing.T) {
	for _, access := range []bool{false, true} {
		for _, saturated := range []bool{false, true} {
			name := "put"
			if access {
				name = "access_bump"
			}
			if saturated {
				name += "_saturated"
			}
			t.Run(name, func(t *testing.T) {
				c := &fakeClock{now: 100}
				if saturated {
					c.now = math.MaxInt64 - 5
				}
				cfg := ccfg1seg(c, 10)
				cfg.AccessOrder = access
				s := store.NewStoreByteArray(true)
				m := NewHTreeCache(s, cfg)
				m.Put(1, 10)
				if access {
					m.Put(2, 20)
				}
				h := m.HeaderRecid()
				m.CloseHandle()
				c.now = 0
				var err error
				m, err = OpenHTreeCache(s, h, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer m.CloseHandle()
				key := int64(2)
				if access {
					key = 1
					if _, ok := m.Get(key); !ok {
						t.Fatal("expected hit")
					}
				} else {
					m.Put(key, 20)
				}
				var stamps []uint64
				m.inner.queues[0].ForEach(func(_ store.Recid, _ uint64, stamp uint64) { stamps = append(stamps, stamp) })
				c.now = 11
				live := m.ContainsKey(key)
				m.ExpireEvict()
				r2CheckCoherent(t, m)
				t.Logf("queue stamps=%v key%d live=%v physical after unbounded sweep=%d", stamps, key, live, m.LenLong())
				if !live && m.LenLong() == 2 {
					t.Fatal("expired suffix remains behind a live tail after reopen")
				}
			})
		}
	}
}

func TestCacheReviewR2FlushPreservesBodyError(t *testing.T) {
	cfg := ccfgPlain(&fakeClock{})
	cfg.MaxSize = 1
	m := NewHTreeCache(store.NewStoreByteArray(true), cfg)
	defer m.CloseHandle()
	m.Put(1, 10)
	m.Put(2, 20)
	body := errors.New("secondary index removal failed")
	flush := errors.New("overflow unavailable")
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](func(_ int64, _, _ *int64, trig bool) error {
		if trig {
			return body
		}
		return nil
	}))
	m.SetOverflow(&probeOverflow{put: func(int64, int64) (int64, bool) { panic(flush) }})
	p := probePanic(func() { m.Put(3, 30) })
	r2CheckCoherent(t, m)
	if m.ContainsKey(3) {
		t.Fatal("aborted write inserted key")
	}
	err, ok := p.(error)
	t.Logf("panic=%v matches body=%v matches flush=%v", p, ok && errors.Is(err, body), ok && errors.Is(err, flush))
	if !ok || !errors.Is(err, body) {
		t.Fatal("overflow panic discarded the primary synchronous-listener error")
	}
}

func TestCacheReviewR2SyncFailureCoherence(t *testing.T) {
	for _, op := range []string{"put", "remove", "replace", "get", "evict"} {
		for _, collision := range []bool{false, true} {
			t.Run(op+map[bool]string{false: "", true: "_collision"}[collision], func(t *testing.T) {
				c := &fakeClock{}
				cfg := ccfg1seg(c, 10)
				cfg.AccessOrder = op == "get"
				if collision {
					cfg.Hasher = probeConstantHasher{}
				}
				m := NewHTreeCache(store.NewStoreByteArray(true), cfg)
				defer m.CloseHandle()
				for k := int64(1); k <= 3; k++ {
					m.Put(k, k*10)
				}
				sentinel := errors.New("failed second victim")
				syncSeen, defSeen, evSeen := 0, 0, 0
				m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](func(k int64, old, new *int64, trig bool) error {
					if trig {
						syncSeen++
						r2CheckCoherent(t, m)
						if syncSeen == 2 {
							return sentinel
						}
					}
					return nil
				}))
				m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](func(k int64, old, new *int64, trig bool) error {
					if trig {
						defSeen++
					}
					return nil
				}))
				m.SetEvictionListener(func(int64, int64, EvictionCause) { evSeen++ })
				c.now = 11
				p := probePanic(func() {
					switch op {
					case "put":
						m.Put(4, 40)
					case "remove":
						m.Remove(3)
					case "replace":
						m.Replace(3, 40)
					case "get":
						m.Get(3)
					case "evict":
						m.ExpireEvict()
					}
				})
				err, ok := p.(error)
				if !ok || !errors.Is(err, sentinel) {
					t.Fatalf("lost error: %v", p)
				}
				r2CheckCoherent(t, m)
				if m.LenLong() != 0 || syncSeen != 3 || defSeen != 3 || evSeen != 3 {
					t.Fatalf("len=%d sync=%d deferred=%d eviction=%d", m.LenLong(), syncSeen, defSeen, evSeen)
				}
			})
		}
	}
}

func TestCacheReviewR2CallbackAllowedPaths(t *testing.T) {
	c := &fakeClock{}
	cfg := ccfgPlain(c)
	cfg.MaxSize = 1
	m := NewHTreeCache(store.NewStoreByteArray(true), cfg)
	defer m.CloseHandle()
	cfg.AccessOrder = true
	ov := NewHTreeCache(store.NewStoreByteArray(true), cfg)
	defer ov.CloseHandle()
	m.SetOverflow(ov)
	ov.Put(8, 80)
	ov.Put(9, 90)
	ovCallbacks := 0
	ov.SetEvictionListener(func(int64, int64, EvictionCause) {
		ovCallbacks++
		if !m.ContainsKey(3) {
			t.Error("cache overflow callback could not read parent")
		}
	})
	evCallbacks := 0
	m.SetEvictionListener(func(int64, int64, EvictionCause) {
		evCallbacks++
		if v, ok := m.Get(3); !ok || v != 30 {
			t.Error("ordinary Get hit rejected")
		}
		if _, ok := m.Get(404); ok {
			t.Error("ordinary Get miss")
		}
		ov.Put(77, 770) // mutation of another cache is legal
	})
	deferred := 0
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](func(_ int64, _, _ *int64, trig bool) error {
		if trig {
			deferred++
			m.Remove(2)
		}
		return nil
	}))
	m.Put(1, 10)
	m.Put(2, 20)
	m.Put(3, 30)
	if evCallbacks != 2 || ovCallbacks == 0 || deferred != 2 {
		t.Fatalf("callbacks eviction=%d overflow=%d deferred=%d", evCallbacks, ovCallbacks, deferred)
	}
	r2CheckCoherent(t, m)
	r2CheckCoherent(t, ov)
}

func TestCacheReviewR2CallbackMutationGuard(t *testing.T) {
	for _, op := range []string{"put", "remove", "replace", "clear", "clear_silent", "expire", "get_access", "get_loader", "get_overflow"} {
		t.Run(op, func(t *testing.T) {
			c := &fakeClock{}
			cfg := ccfg1seg(c, 10)
			cfg.AccessOrder = op == "get_access"
			m := NewHTreeCache(store.NewStoreByteArray(true), cfg)
			defer m.CloseHandle()
			m.Put(1, 10)
			c.now = 11
			var caught any
			m.SetEvictionListener(func(int64, int64, EvictionCause) {
				caught = probePanic(func() {
					switch op {
					case "put":
						m.Put(2, 20)
					case "remove":
						m.Remove(2)
					case "replace":
						m.Replace(2, 20)
					case "clear":
						m.Clear()
					case "clear_silent":
						m.ClearWithoutNotification()
					case "expire":
						m.ExpireEvict()
					case "get_access":
						m.Get(2)
					case "get_loader":
						m.ValueLoader(func(int64) (int64, bool) { return 20, true })
						m.Get(2)
					case "get_overflow":
						m.SetOverflow(&probeOverflow{get: func(int64) (int64, bool) { return 20, true }})
						m.Get(2)
					}
				})
			})
			m.ExpireEvict()
			if caught == nil {
				t.Fatal("mutation allowed")
			}
			m.Put(2, 20) // callback guard cleared
			r2CheckCoherent(t, m)
		})
	}
}

func TestCacheReviewR2RollbackSaturationWithoutReopen(t *testing.T) {
	c := &fakeClock{now: math.MaxInt64 - 5}
	cfg := ccfg1seg(c, 10)
	cfg.AccessOrder = true
	m := NewHTreeCache(store.NewStoreByteArray(true), cfg)
	defer m.CloseHandle()
	m.Put(1, 10)
	m.Put(2, 20)
	c.now = 0
	m.Get(1)
	m.Replace(2, 21)
	m.PutIfAbsent(1, 11)
	m.inner.queues[0].ForEach(func(_ store.Recid, _, stamp uint64) {
		if stamp != math.MaxInt64 {
			t.Fatalf("stamp=%d", stamp)
		}
	})
	c.now = math.MaxInt64
	m.ExpireEvict()
	if !m.ContainsKey(1) || !m.ContainsKey(2) {
		t.Fatal("saturated deadline expired")
	}
	r2CheckCoherent(t, m)
}

func TestCacheReviewR2RetirementAllMutators(t *testing.T) {
	for _, op := range []string{"put", "remove", "replace"} {
		t.Run(op, func(t *testing.T) {
			c := &fakeClock{}
			cfg := ccfg1seg(c, 10)
			s := store.NewStoreByteArray(true)
			m := NewHTreeCache(s, cfg)
			m.Put(1, 10)
			c.now = 5
			m.Put(2, 20)
			c.now = 11
			var reopened *HTreeCache[int64, int64]
			m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](func(_ int64, _, _ *int64, trig bool) error {
				if trig {
					m.CloseHandle()
					var err error
					reopened, err = OpenHTreeCache(s, m.HeaderRecid(), cfg)
					return err
				}
				return nil
			}))
			p := probePanic(func() {
				switch op {
				case "put":
					m.Put(2, 99)
				case "remove":
					m.Remove(2)
				case "replace":
					m.Replace(2, 99)
				}
			})
			if p == nil || reopened == nil {
				t.Fatalf("panic=%v reopened=%v", p, reopened)
			}
			defer reopened.CloseHandle()
			if v, ok := reopened.Get(2); !ok || v != 20 {
				t.Fatalf("retired mutation changed key: %d %v", v, ok)
			}
			r2CheckCoherent(t, reopened)
		})
	}
}
