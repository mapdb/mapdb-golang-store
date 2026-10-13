// Command bench is the standalone benchmark runner. It mirrors the Java JMH
// runner of mapdb-java-store: the same subjects (where Go has them), the
// same datatypes and workload names, throughput reported in Mentries/s, so
// rows are comparable across implementations.
//
//	go run ./cmd/bench -mode dev                 # wiring smoke, seconds
//	go run ./cmd/bench -mode full -out results   # 1M entries, 1/8/32 goroutines
//	go run ./cmd/bench -mode store               # store-level locked reads (M3b)
//
// Output: <out>/<date>-<label>.csv (one row per subject×datatype×workload×
// threads) and <out>/<date>-<label>-table.md (pivoted table). Without -out
// only the table is printed.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mapdb/mapdb5-go/internal/benchkit"
)

type config struct {
	mode      string
	size      int
	subjects  []string
	types     []benchkit.DataType
	workloads []string
	threads   []int
	warmup    int
	iters     int
	dur       time.Duration
	out       string
	label     string
}

type row struct {
	subject, datatype, workload string
	threads                     int
	mops                        float64 // mean Mentries/s over measured iterations
	minMops, maxMops            float64
	allocPerOp                  float64 // heap bytes per entry (t1 only, else -1)
}

func main() {
	var c config
	var subjects, types, workloads, threads string
	flag.StringVar(&c.mode, "mode", "dev", "dev | full | scale | store")
	flag.IntVar(&c.size, "size", 0, "entries (default per mode: dev 100k, full 1M, scale 10M)")
	flag.StringVar(&subjects, "subjects", "", "comma list of subject keys (default all)")
	flag.StringVar(&types, "types", "LONG,STRING", "comma list of LONG,STRING")
	flag.StringVar(&workloads, "workloads", "", "comma list of workloads (default all)")
	flag.StringVar(&threads, "threads", "", "comma list of goroutine counts for hot point workloads (default per mode)")
	flag.IntVar(&c.warmup, "warmup", -1, "warmup iterations")
	flag.IntVar(&c.iters, "iters", -1, "measured iterations")
	flag.DurationVar(&c.dur, "dur", 0, "duration of one iteration")
	flag.StringVar(&c.out, "out", "", "results directory (CSV + table); empty = print only")
	flag.StringVar(&c.label, "label", "", "result file label (default = mode)")
	flag.Parse()

	defaults(&c, threads)
	if subjects != "" {
		c.subjects = strings.Split(subjects, ",")
	} else {
		for _, s := range benchkit.Subjects {
			c.subjects = append(c.subjects, s.Key)
		}
	}
	for _, t := range strings.Split(types, ",") {
		c.types = append(c.types, benchkit.DataType(strings.ToUpper(strings.TrimSpace(t))))
	}
	if workloads != "" {
		c.workloads = strings.Split(workloads, ",")
	} else {
		c.workloads = benchkit.AllWorkloads
	}
	if c.label == "" {
		c.label = c.mode
	}

	// Fixed runtime settings, the Go analog of the Java harness's fixed JVM
	// flags: default GC target, GOMAXPROCS = all cores.
	debug.SetGCPercent(100)
	fmt.Fprintf(os.Stderr, "go %s GOMAXPROCS=%d mode=%s size=%d warmup=%d×%s iters=%d×%s\n",
		runtime.Version(), runtime.GOMAXPROCS(0), c.mode, c.size, c.warmup, c.dur, c.iters, c.dur)

	var rows []row
	if c.mode == "store" {
		rows = runStore(c)
	} else {
		rows = runMaps(c)
	}
	table := pivot(rows, c)
	fmt.Print(table)
	if c.out != "" {
		if err := write(c, rows, table); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func defaults(c *config, threads string) {
	switch c.mode {
	case "dev":
		setInt(&c.size, 100_000)
		setInt(&c.warmup, 1)
		setInt(&c.iters, 1)
		if c.dur == 0 {
			c.dur = 300 * time.Millisecond
		}
	case "full", "store":
		setInt(&c.size, 1_000_000)
		setInt(&c.warmup, 3)
		setInt(&c.iters, 5)
		if c.dur == 0 {
			c.dur = 2 * time.Second
		}
	case "scale":
		setInt(&c.size, 10_000_000)
		setInt(&c.warmup, 2)
		setInt(&c.iters, 3)
		if c.dur == 0 {
			c.dur = 2 * time.Second
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown -mode", c.mode)
		os.Exit(2)
	}
	if threads == "" {
		threads = "1,8,32"
		if c.mode == "dev" {
			threads = "1,8"
		}
	}
	for _, s := range strings.Split(threads, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			fmt.Fprintln(os.Stderr, "bad -threads", threads)
			os.Exit(2)
		}
		c.threads = append(c.threads, n)
	}
}

func setInt(p *int, v int) {
	if *p <= 0 {
		*p = v
	}
}

// hot point workloads run at every thread count (Java: getHit, overwrite,
// mixed90r10w); the rest at one goroutine.
func threadsFor(w string, s benchkit.Subject, c config) []int {
	if !s.MultiThread {
		return []int{1}
	}
	switch w {
	case benchkit.GetHit, benchkit.Overwrite, benchkit.Mixed90r10w:
		return c.threads
	}
	return []int{1}
}

func runStore(c config) []row {
	var rows []row
	for _, ss := range benchkit.StoreSubjects {
		st := ss.Build()
		recids := benchkit.PopulateStore(st, c.size)
		for _, t := range c.threads {
			rows = append(rows, measure(c, ss.ID, "LONG", benchkit.StoreGetWorkload, t, func() benchkit.Sample {
				return benchkit.RunStoreGet(st, recids, t, c.dur)
			}))
		}
		_ = st.Close()
	}
	return rows
}

func runMaps(c config) []row {
	var rows []row
	for _, dt := range c.types {
		switch dt {
		case benchkit.Long:
			d := benchkit.LongDataset(c.size)
			for _, key := range c.subjects {
				s := mustSubject(key)
				rows = append(rows, runSubject(c, s, string(dt), s.BuildLong, d)...)
			}
		case benchkit.String:
			d := benchkit.StringDataset(c.size)
			for _, key := range c.subjects {
				s := mustSubject(key)
				rows = append(rows, runSubject(c, s, string(dt), s.BuildString, d)...)
			}
		default:
			fmt.Fprintln(os.Stderr, "unknown datatype", dt)
			os.Exit(2)
		}
	}
	return rows
}

func mustSubject(key string) benchkit.Subject {
	s, err := benchkit.SubjectByKey(strings.TrimSpace(key))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return s
}

func runSubject[K, V any](c config, s benchkit.Subject, dt string, build func() benchkit.Map[K, V], d *benchkit.Dataset[K, V]) []row {
	var rows []row
	var points []string
	for _, w := range c.workloads {
		if benchkit.IsPoint(w) {
			points = append(points, w)
		}
	}
	// One populated map per subject×datatype serves every per-op workload
	// and iterate (Java: Benchmark-scoped trial state; overwrites keep the
	// size constant).
	needPopulated := len(points) > 0 || contains(c.workloads, benchkit.Iterate)
	var m benchkit.Map[K, V]
	if needPopulated {
		m = build()
		benchkit.Populate(m, d)
		defer m.Close()
	}
	for _, w := range c.workloads {
		switch {
		case benchkit.IsPoint(w):
			for _, t := range threadsFor(w, s, c) {
				rows = append(rows, measure(c, s.ID, dt, w, t, func() benchkit.Sample {
					return benchkit.RunPoint(w, m, d, t, c.dur)
				}))
			}
		case w == benchkit.Iterate:
			if !s.Ordered {
				continue
			}
			rows = append(rows, measure(c, s.ID, dt, w, 1, func() benchkit.Sample {
				smp, err := benchkit.RunIterate(m, d, c.dur)
				if err != nil {
					panic(err)
				}
				return smp
			}))
		case w == benchkit.PutSequential || w == benchkit.PutRandom:
			seq := w == benchkit.PutSequential
			rows = append(rows, measure(c, s.ID, dt, w, 1, func() benchkit.Sample {
				return benchkit.RunLoad(build, d, seq)
			}))
		default:
			fmt.Fprintln(os.Stderr, "unknown workload", w)
			os.Exit(2)
		}
	}
	return rows
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func measure(c config, subject, dt, w string, threads int, run func() benchkit.Sample) row {
	for i := 0; i < c.warmup; i++ {
		run()
	}
	r := row{subject: subject, datatype: dt, workload: w, threads: threads, allocPerOp: -1,
		minMops: 1e300}
	var sum float64
	var ops int64
	var alloc uint64
	for i := 0; i < c.iters; i++ {
		smp := run()
		v := smp.PerSec() / 1e6
		sum += v
		if v < r.minMops {
			r.minMops = v
		}
		if v > r.maxMops {
			r.maxMops = v
		}
		ops += smp.Ops
		alloc += smp.AllocBytes
	}
	r.mops = sum / float64(c.iters)
	if threads == 1 && ops > 0 {
		r.allocPerOp = float64(alloc) / float64(ops)
	}
	fmt.Fprintf(os.Stderr, "%-38s %-6s %-14s t%-3d %9.3f Mops/s  [%0.3f..%0.3f]  alloc/op %s\n",
		subject, dt, w, threads, r.mops, r.minMops, r.maxMops, allocStr(r.allocPerOp))
	return r
}

func allocStr(v float64) string {
	if v < 0 {
		return "-"
	}
	return strconv.FormatFloat(v, 'f', 1, 64)
}

func pivot(rows []row, c config) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## %s — %d entries, Mentries/s (mean of %d × %s)\n\n", c.mode, c.size, c.iters, c.dur)
	type col struct {
		w string
		t int
	}
	types := map[string]bool{}
	for _, r := range rows {
		types[r.datatype] = true
	}
	var dts []string
	for t := range types {
		dts = append(dts, t)
	}
	sort.Strings(dts)
	for _, dt := range dts {
		var cols []col
		seen := map[col]bool{}
		var subjects []string
		seenS := map[string]bool{}
		cell := map[string]map[col]row{}
		for _, r := range rows {
			if r.datatype != dt {
				continue
			}
			k := col{r.workload, r.threads}
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
			if !seenS[r.subject] {
				seenS[r.subject] = true
				subjects = append(subjects, r.subject)
				cell[r.subject] = map[col]row{}
			}
			cell[r.subject][k] = r
		}
		fmt.Fprintf(&b, "### %s\n\n| subject |", dt)
		for _, k := range cols {
			fmt.Fprintf(&b, " %s t%d |", k.w, k.t)
		}
		b.WriteString("\n| --- |")
		for range cols {
			b.WriteString(" ---: |")
		}
		b.WriteString("\n")
		for _, s := range subjects {
			fmt.Fprintf(&b, "| %s |", s)
			for _, k := range cols {
				if r, ok := cell[s][k]; ok {
					fmt.Fprintf(&b, " %s |", fmtMops(r.mops))
				} else {
					b.WriteString(" - |")
				}
			}
			b.WriteString("\n")
		}
		b.WriteString("\nHeap bytes allocated per entry (t1):\n\n| subject |")
		var t1 []col
		for _, k := range cols {
			if k.t == 1 {
				t1 = append(t1, k)
				fmt.Fprintf(&b, " %s |", k.w)
			}
		}
		b.WriteString("\n| --- |")
		for range t1 {
			b.WriteString(" ---: |")
		}
		b.WriteString("\n")
		for _, s := range subjects {
			fmt.Fprintf(&b, "| %s |", s)
			for _, k := range t1 {
				if r, ok := cell[s][k]; ok {
					fmt.Fprintf(&b, " %s |", allocStr(r.allocPerOp))
				} else {
					b.WriteString(" - |")
				}
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	return b.String()
}

func fmtMops(v float64) string {
	if v >= 100 {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func write(c config, rows []row, table string) error {
	if err := os.MkdirAll(c.out, 0o755); err != nil {
		return err
	}
	stem := filepath.Join(c.out, time.Now().Format("2006-01-02")+"-"+c.label)
	var b strings.Builder
	b.WriteString("subject,datatype,workload,threads,size,mops,min_mops,max_mops,alloc_bytes_per_op,goversion,gomaxprocs\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "%q,%s,%s,%d,%d,%.4f,%.4f,%.4f,%s,%s,%d\n", r.subject, r.datatype, r.workload,
			r.threads, c.size, r.mops, r.minMops, r.maxMops, allocStr(r.allocPerOp),
			runtime.Version(), runtime.GOMAXPROCS(0))
	}
	if err := os.WriteFile(stem+".csv", []byte(b.String()), 0o644); err != nil {
		return err
	}
	return os.WriteFile(stem+"-table.md", []byte(table), 0o644)
}
