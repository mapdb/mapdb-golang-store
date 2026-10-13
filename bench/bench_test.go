// Package bench holds the testing.B benchmarks. They share the harness
// with cmd/bench (internal/benchkit): same subjects, dataset and workload
// names. The dataset size defaults to 100k entries; set
// MAPDB5_BENCH_SIZE to change it.
package bench

import (
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/mapdb/mapdb5-go/internal/benchkit"
)

func benchSize() int {
	if v, err := strconv.Atoi(os.Getenv("MAPDB5_BENCH_SIZE")); err == nil && v > 0 {
		return v
	}
	return 100_000
}

var (
	longData   *benchkit.Dataset[int64, int64]
	stringData *benchkit.Dataset[string, string]
)

func longDataset() *benchkit.Dataset[int64, int64] {
	if longData == nil {
		longData = benchkit.LongDataset(benchSize())
	}
	return longData
}

func stringDataset() *benchkit.Dataset[string, string] {
	if stringData == nil {
		stringData = benchkit.StringDataset(benchSize())
	}
	return stringData
}

func BenchmarkLong(b *testing.B) {
	for _, s := range benchkit.Subjects {
		b.Run(s.Key, func(b *testing.B) { runAll(b, s, s.BuildLong, longDataset()) })
	}
}

func BenchmarkString(b *testing.B) {
	for _, s := range benchkit.Subjects {
		b.Run(s.Key, func(b *testing.B) { runAll(b, s, s.BuildString, stringDataset()) })
	}
}

// runAll gives every workload (and, for RunParallel, every -cpu value)
// a FRESH populated map, like JMH's per-benchmark fork with trial-scoped
// state: a workload never measures the state an earlier one churned.
func runAll[K, V any](b *testing.B, s benchkit.Subject, build func() benchkit.Map[K, V], d *benchkit.Dataset[K, V]) {
	populated := func(b *testing.B) benchkit.Map[K, V] {
		b.StopTimer()
		m := build()
		benchkit.Populate(m, d)
		b.Cleanup(m.Close)
		b.StartTimer()
		return m
	}
	for _, w := range benchkit.PointWorkloads {
		b.Run(w, func(b *testing.B) {
			mp := populated(b)
			b.ReportAllocs()
			b.ResetTimer()
			if s.MultiThread {
				b.RunParallel(func(pb *testing.PB) {
					op := benchkit.NewOp(w, mp, d)
					var acc uint64
					for pb.Next() {
						if op() {
							acc++
						}
					}
					benchkit.Sink.Add(acc)
				})
				return
			}
			op := benchkit.NewOp(w, mp, d)
			var acc uint64
			for i := 0; i < b.N; i++ {
				if op() {
					acc++
				}
			}
			benchkit.Sink.Add(acc)
		})
	}
	if s.Ordered {
		b.Run(benchkit.Iterate, func(b *testing.B) {
			mp := populated(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sum, _ := mp.IterateChecksum()
				benchkit.Sink.Add(sum)
			}
			b.ReportMetric(float64(b.N)*float64(d.N)/b.Elapsed().Seconds()/1e6, "Mentries/s")
		})
	}
	for _, w := range []string{benchkit.PutSequential, benchkit.PutRandom} {
		b.Run(w, func(b *testing.B) {
			b.ReportAllocs()
			var total float64
			for i := 0; i < b.N; i++ {
				smp := benchkit.RunLoad(build, d, w == benchkit.PutSequential)
				total += smp.Elapsed.Seconds()
			}
			b.ReportMetric(float64(b.N)*float64(d.N)/total/1e6, "Mentries/s")
		})
	}
}

func BenchmarkStoreGet(b *testing.B) {
	for _, ss := range benchkit.StoreSubjects {
		b.Run(ss.ID, func(b *testing.B) {
			st := ss.Build()
			defer st.Close()
			recids := benchkit.PopulateStore(st, benchSize())
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.IntN(len(recids))
				var acc uint64
				for pb.Next() {
					acc += benchkit.StoreGet(st, recids[i])
					i++
					if i == len(recids) {
						i = 0
					}
				}
				benchkit.Sink.Add(acc)
			})
		})
	}
}
