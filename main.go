package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/andreyvit/diff"
)

func assert(err error) {
	if err != nil {
		panic(err)
	}
}

func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// allocator maps string keys to uint32 "slots", which are unique integers
// allocated from a counter. They are called slots because they map to space in
// the accumulator below.
type allocator struct {
	next    uint32
	storage map[string]uint32
}

func newAllocator() *allocator {
	return &allocator{
		storage: map[string]uint32{},
	}
}

func (k *allocator) alloc(s string) uint32 {
	slot, ok := k.storage[s]
	if !ok {
		safeKey := string(append([]byte(nil), s...))
		k.storage[safeKey] = k.next
		slot = k.next

		// I guess we'll just have to live with not checking for overflow here.
		k.next++
	}

	return slot
}

// accumulator stores the intermediate state for aggregates
type accumulator struct {
	max      []float32
	min      []float32
	sum      []float64
	count    []uint32
	occupied []bool
}

func newAccumulator() *accumulator {
	return &accumulator{
		max:      make([]float32, 1<<5),
		min:      make([]float32, 1<<5),
		sum:      make([]float64, 1<<5),
		count:    make([]uint32, 1<<5),
		occupied: make([]bool, 1<<5),
	}
}

// ensure checks that there is enough space in the accumulator the requested
// slot and reallocs the aggregate storage slices if not.
func (a *accumulator) ensure(slot uint32) {
	if len(a.occupied)-1 > int(slot) {
		return
	}

	newMax := make([]float32, len(a.occupied)*2)
	copy(newMax, a.max)
	a.max = newMax

	newMin := make([]float32, len(a.occupied)*2)
	copy(newMin, a.min)
	a.min = newMin

	newSum := make([]float64, len(a.occupied)*2)
	copy(newSum, a.sum)
	a.sum = newSum

	newCount := make([]uint32, len(a.occupied)*2)
	copy(newCount, a.count)
	a.count = newCount

	newOccupied := make([]bool, len(a.occupied)*2)
	copy(newOccupied, a.occupied)
	a.occupied = newOccupied
}

// task wraps an input, allocator, and accumulator to run our hard-coded query
// over a subset of the input.
type task struct {
	alloc *allocator
	accum *accumulator
	input io.Reader
}

func newTask(input io.Reader) *task {
	return &task{
		alloc: newAllocator(),
		accum: newAccumulator(),
		input: input,
	}
}

func (t *task) run() {
	scanner := bufio.NewScanner(t.input)

	for scanner.Scan() {
		var (
			line = scanner.Bytes()
			end  int
		)

		// Start from the end of the line for what I hope is a mild performance
		// win. I didn't actually benchmark this, but we're only searching for
		// a few characters from the end of the line.
		//
		// I could cheat by starting from the end of the line minus 7 to do
		// fewer iterations in the common case...
		for i := len(line) - 1; i > 0; i-- {
			if line[i] == ';' {
				end = i + 1
			}
		}

		var (
			key  = bytesToString(line[:end-1])
			val  = bytesToString(line[end:])
			slot = t.alloc.alloc(key)
		)

		t.accum.ensure(slot)

		v, err := strconv.ParseFloat(val, 32)
		assert(err)

		v32 := float32(v)

		t.accum.sum[slot] += v
		t.accum.count[slot]++

		if !t.accum.occupied[slot] {
			t.accum.min[slot] = v32
			t.accum.max[slot] = v32
			t.accum.occupied[slot] = true
			continue
		}

		if v32 < t.accum.min[slot] {
			t.accum.min[slot] = v32
		}

		if v32 > t.accum.max[slot] {
			t.accum.max[slot] = v32
		}
	}

	assert(scanner.Err())
}

// merge takes the intermediate results of the other task `ot` and merges it
// with the intermediate results of this task `t`.
func (t *task) merge(ot *task) {
	for key, os := range ot.alloc.storage {
		ts := t.alloc.alloc(key)
		t.accum.ensure(ts)

		t.accum.count[ts] += ot.accum.count[os]
		t.accum.sum[ts] += ot.accum.sum[os]

		if !t.accum.occupied[ts] {
			t.accum.min[ts] = ot.accum.min[os]
			t.accum.min[ts] = ot.accum.min[os]
			t.accum.occupied[ts] = true
			continue
		}

		if ot.accum.min[os] < t.accum.min[ts] {
			t.accum.min[ts] = ot.accum.min[os]
		}

		if ot.accum.max[os] > t.accum.max[ts] {
			t.accum.max[ts] = ot.accum.max[os]
		}
	}
}

func splitIntoTasks(path string, taskCount int) []*task {
	f, err := os.Open(path)
	assert(err)
	defer f.Close()

	fi, err := f.Stat()
	assert(err)

	size := fi.Size()

	step := size / int64(taskCount)

	type split struct {
		begin, end int64
	}

	var splits []split

	var base = int64(0)
	for {
		targetEnd := base + step
		if targetEnd+step >= size {
			splits = append(splits, split{begin: int64(base), end: size})
			break
		}

		_, err := f.Seek(int64(targetEnd), 0)
		assert(err)

		scanner := bufio.NewScanner(bufio.NewReader(f))
		scanner.Split(bufio.ScanBytes)

		// We have to keep scanning from `targetEnd` to the end of the next
		// full record.
		for scanner.Scan() {
			targetEnd++
			if scanner.Bytes()[0] == '\n' {
				break
			}
		}

		splits = append(splits, split{
			begin: base,
			end:   targetEnd,
		})

		base = targetEnd
	}

	var tasks []*task

	for _, s := range splits {
		f, err := os.Open(path)
		assert(err)
		_, err = f.Seek(s.begin, 0)
		assert(err)

		sr := io.NewSectionReader(f, s.begin, s.end-s.begin)
		// buffer size chosen somewhat arbitrarily but it is reasonable
		br := bufio.NewReaderSize(sr, 1<<19)

		tasks = append(tasks, newTask(br))
	}

	return tasks
}

func executeTasks(tasks []*task) {
	var wg sync.WaitGroup
	for _, t := range tasks {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.run()
		}()
	}

	wg.Wait()
}

func mergeAndGenerateResult(tasks []*task) string {
	for i, t := range tasks {
		if i == 0 {
			continue
		}

		tasks[0].merge(t)
	}

	t := tasks[0]

	var keys []string
	for k := range t.alloc.storage {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var lines []string
	for _, key := range keys {
		slot := t.alloc.alloc(key)
		min := t.accum.min[slot]
		avg := float32(t.accum.sum[slot] / float64(t.accum.count[slot]))
		max := t.accum.max[slot]

		lines = append(lines, fmt.Sprintf(
			"%q;%s;%s;%s;%d",
			key,
			formatRemoveTrailingZero(min),
			formatRemoveTrailingZero(max),
			formatRemoveTrailingZero(avg),
			t.accum.count[slot]))
	}

	return strings.Join(lines, "\n")
}

func compareResult(result string, compareToPath string) {
	compareFile, err := os.Open(compareToPath)
	assert(err)

	expectedBuf, err := io.ReadAll(compareFile)
	assert(err)

	expected := string(expectedBuf)

	fmt.Println(diff.LineDiff(
		diff.TrimLinesInString(expected),
		diff.TrimLinesInString(result)))
}

func formatRemoveTrailingZero(f float32) string {
	s := fmt.Sprintf("%.1f", f)
	if strings.HasSuffix(s, ".0") {
		return s[:len(s)-2]
	}

	return s
}

func Run(path string, taskCount int, compareToPath string) string {
	tasks := splitIntoTasks(path, taskCount)

	executeTasks(tasks)

	result := mergeAndGenerateResult(tasks)

	if compareToPath != "" {
		compareResult(result, compareToPath)
	}

	return result
}

func main() {
	fmt.Println(Run("measurements.txt", 128, ""))
}
