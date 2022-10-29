package tickque

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
)

//gocyclo:ignore
func TestTickque_Routine(t *testing.T) {
	handler := func(job *Job) error {
		if job.Type == "beta" {
			return errors.New("beta")
		}
		return nil
	}

	sc := slog.NewScavenger()
	tq := NewTickque(sc)
	if tq.Tick(10, handler) != 0 {
		t.Fatal(`tq.Tick(10, handler) != 0`)
	}
	if sc.Len() != 0 {
		t.Fatal(`sc.Len() != 0`)
	}

	for _, v := range []int{1, 3, 9, 10, 11, 19, 100} {
		for i := 0; i < v; i++ {
			tq.AddJob(fmt.Sprintf("alpha-%d-%d", v, i), live.Nil)
		}
		var c1 int
		for tq.NumPendingJobs() > 0 {
			c2 := tq.NumPendingJobs()
			processed := tq.Tick(10, handler)
			var expected int
			if v%10 == 0 || c2 >= 10 {
				expected = 10
			} else {
				expected = v % 10
			}
			if processed != expected {
				t.Fatalf("processed != expected. v: %d, numPendingJobs: %d, processed: %d",
					v, tq.NumPendingJobs(), processed)
			}
			c1++
		}
		if c1 != (v+9)/10 {
			t.Fatal("c1 != (v+9)/10")
		}
	}

	for _, v := range []int{1, 3, 9, 10, 11, 19, 100} {
		sc.Reset()
		for i := 0; i < v; i++ {
			tq.AddJob("beta", live.Nil)
		}
		for tq.NumPendingJobs() > 0 {
			tq.Tick(10, handler)
		}
		if sc.Len() != v {
			t.Fatal(`sc.Len() != v`)
		}
		failed := sc.Filter(func(level, msg string) bool {
			return strings.Contains(msg, "a tickque job failed")
		})
		if failed.Len() != v {
			t.Fatal(`failed.Len() != v`)
		}
	}
}

func TestTickque_Panic(t *testing.T) {
	var n1 int
	handler := func(job *Job) error {
		if job.Type != fmt.Sprint(n1) {
			t.Fatal("job.Type != fmt.Sprint(n1)")
		}
		n1++
		if n1 == 2 {
			panic("beta")
		}
		return nil
	}

	sc := slog.NewScavenger()
	tq := NewTickque(sc)
	for i := 0; i < 15; i++ {
		tq.AddJob(fmt.Sprint(i), live.Nil)
	}

	if processed := tq.Tick(10, handler); processed != 10 {
		t.Fatal("processed != 10")
	}
	if sc.Len() != 1 {
		t.Fatal(`sc.Len() != 1`)
	}
	if yes := sc.StringExists(", panic:"); !yes {
		t.Fatal("panic not detected")
	}
	if tq.NumPendingJobs() != 5 {
		t.Fatal("tq.NumPendingJobs() != 5")
	}
	if n1 != 10 {
		t.Fatal("n1 != 10")
	}

	if processed := tq.Tick(10, handler); processed != 5 {
		t.Fatal("processed != 5")
	}
	if sc.Len() != 1 {
		t.Fatal(`sc.Len() != 1`)
	}
	if tq.NumPendingJobs() != 0 {
		t.Fatal("tq.NumPendingJobs() != 0")
	}
	if n1 != 15 {
		t.Fatal("n1 != 15")
	}

	func() {
		defer func() {
			_ = recover()
		}()
		tq.Tick(0, handler)
		t.Fatal("Tick should panic")
	}()
}

//gocyclo:ignore
func TestTickque_Postpone(t *testing.T) {
	var n1 int
	tq := NewTickque(nil)
	handler := func(job *Job) error {
		switch job.Type {
		case "0":
			if job.RetryNumber() != int32(n1) {
				t.Fatal("job.RetryNumber() != int32(n1)")
			}
			n1++
		}
		tq.Postpone(job)
		return nil
	}

	if tq.Tick(1, handler) != 0 {
		t.Fatal("tq.Tick(1, handler) != 0")
	}

	tq.AddJob("0", live.Nil)
	for i := 0; i < 10; i++ {
		tq.Tick(10, handler)
		if n1 != i+1 {
			t.Fatal("n1 != i+1")
		}
	}

	for i := 1; i < 10; i++ {
		tq.AddJob(fmt.Sprint(i), live.Nil)
	}
	for i := 1; i < 50; i++ {
		if i <= 10 {
			if tq.Tick(i, handler) != i {
				t.Fatal("tq.Tick(i, handler) != i")
			}
		} else {
			if tq.Tick(i, handler) != 10 {
				t.Fatal("tq.Tick(i, handler) != 10")
			}
		}
	}

	for i := 0; i < 10; i++ {
		tq.AddJob(fmt.Sprint(100+i), live.Nil)
	}
	for i := 1; i < 50; i++ {
		if i <= 20 {
			if tq.Tick(i, handler) != i {
				t.Fatal("tq.Tick(i, handler) != i")
			}
		} else {
			if tq.Tick(i, handler) != 20 {
				t.Fatal("tq.Tick(i, handler) != 20")
			}
		}
	}
}

func TestTickque_Burst(t *testing.T) {
	rand.Seed(time.Now().Unix())
	const numThreads = 4
	var threadCounters [numThreads]int64
	var remaining, counter, numPanics, numRetries int64
	var tq *Tickque
	handler := func(job *Job) error {
		atomic.AddInt64(&counter, 1)
		if job.Hint() != 0 {
			slot := scatter(uint64(job.Hint())) % (tq.burst.nq)
			atomic.AddInt64(&threadCounters[slot], 1)
		}
		n := rand.Intn(100)
		switch {
		case n < 5:
			atomic.AddInt64(&remaining, -1)
			atomic.AddInt64(&numPanics, 1)
			panic("boom!")
		case n < 20:
			atomic.AddInt64(&numRetries, 1)
			tq.Postpone(job)
		default:
			atomic.AddInt64(&remaining, -1)
		}
		return nil
	}

	const total = 100000
	tq = NewTickque(slog.NewDumbLogger(), WithNumBurstThreads(numThreads))
	for i := 0; i < total; i++ {
		n := rand.Intn(100)
		switch {
		case n < 5 || i == 0:
			tq.AddJob(fmt.Sprint(i), live.Nil)
		default:
			tq.AddBurstJob(int64(i), fmt.Sprint(i), live.Nil)
		}
	}

	remaining = total
	var numProcessed int64
	for tq.NumPendingJobs() > 0 {
		n := tq.Tick(1+rand.Intn(300), handler)
		numProcessed += int64(n)
	}

	if atomic.LoadInt64(&remaining) != 0 {
		t.Fatal("remaining != 0", atomic.LoadInt64(&remaining))
	}
	if atomic.LoadInt64(&counter) != numProcessed {
		t.Fatal("counter != numProcessed")
	}
	if atomic.LoadInt64(&counter) != total+atomic.LoadInt64(&numRetries) {
		t.Fatal("counter != total+numRetries")
	}

	t.Logf("thread counters: %v", threadCounters)

	func() {
		defer func() {
			_ = recover()
		}()
		NewTickque(nil, WithNumBurstThreads(-10))
		t.Fatal(`WithNumBurstThreads should panic`)
	}()

	func() {
		defer func() {
			_ = recover()
		}()
		NewTickque(nil, WithNumBurstThreads(100))
		t.Fatal(`WithNumBurstThreads should panic`)
	}()

	func() {
		defer func() {
			_ = recover()
		}()
		NewTickque(nil).AddBurstJob(1, "beta", live.Nil)
		t.Fatal(`AddBurstJob should panic`)
	}()

	func() {
		defer func() {
			_ = recover()
		}()
		NewTickque(nil, WithNumBurstThreads(1)).AddBurstJob(0, "beta", live.Nil)
		t.Fatal(`AddBurstJob should panic`)
	}()
}

//gocyclo:ignore
func TestTickque_Shutdown(t *testing.T) {
	var n1 int
	handler1 := func(job *Job) error {
		n1++
		return nil
	}
	logger := slog.NewDumbLogger()
	tq1 := NewTickque(logger)
	data := []int{0, 3, 9, 10, 11, 19, 100}
	for _, v := range data {
		tq1.AddJob(fmt.Sprintf("alpha-%d", v), live.WrapInt(v))
	}
	if total, err := tq1.Shutdown(context.Background(), handler1); err != nil {
		t.Fatal(err)
	} else if total != len(data) {
		t.Fatal("total != len(data)", total)
	} else if n1 != len(data) {
		t.Fatal("n1 != len(data)")
	}

	var n2 int
	handler2 := func(job *Job) error {
		n2++
		if v := job.Data.Int(); v == 10 {
			panic(v)
		}
		return nil
	}
	tq2 := NewTickque(logger)
	for _, v := range data {
		tq2.AddJob(fmt.Sprintf("alpha-%d", v), live.WrapInt(v))
	}
	if total, err := tq2.Shutdown(context.Background(), handler2); err != nil {
		t.Fatal(err)
	} else if total != len(data) {
		t.Fatal("total != len(data)")
	} else if n2 != len(data) {
		t.Fatal("n2 != len(data)")
	}

	var n3 int
	handler3 := func(job *Job) error {
		n3++
		if v := job.Data.Int(); v >= 10 {
			panic(v)
		}
		return nil
	}
	tq3 := NewTickque(logger)
	for _, v := range data {
		tq3.AddJob(fmt.Sprintf("alpha-%d", v), live.WrapInt(v))
	}
	if total, err := tq3.Shutdown(context.Background(), handler3); err != nil {
		t.Fatal(err)
	} else if total != len(data) {
		t.Fatal("total != len(data)")
	} else if n3 != len(data) {
		t.Fatal("n3 != len(data)")
	}

	var n4 int
	handler4 := func(job *Job) error {
		n4++
		if n4 == 2 {
			time.Sleep(time.Millisecond * 20)
		}
		return nil
	}
	tq4 := NewTickque(logger)
	for i := 0; i < 1000; i++ {
		tq4.AddJob("beta", live.Nil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if processed, err := tq4.Shutdown(ctx, handler4); err == nil {
		t.Fatal(`err should not be nil`)
	} else if !os.IsTimeout(err) {
		t.Fatal(`!os.IsTimeout(err)`)
	} else if processed != 2 {
		t.Fatal(`processed != 2`)
	}
}

func TestTickque_Recycle(t *testing.T) {
	var n int32
	handler := func(job *Job) error {
		atomic.AddInt32(&n, 1)
		Recycle(job)
		return nil
	}
	tq := NewTickque(nil, WithNumBurstThreads(8))
	for i := 0; i < 100; i++ {
		tq.AddJob("1", live.Nil)
		go func() {
			for j := 0; j < 100; j++ {
				tq.AddBurstJob(rand.Int63(), "2", live.Nil)
			}
		}()
	}
	var sum int
	for sum != 10100 {
		runtime.Gosched()
		sum += tq.Tick(1000, handler)
	}
	if atomic.LoadInt32(&n) != 10100 {
		t.Fatal("atomic.LoadInt32(&n) != 10100")
	}

	time.Sleep(time.Millisecond * 100)
	sum += tq.Tick(1000, handler)
	if sum != 10100 {
		t.Fatal("sum != 10100")
	}
}

func TestTickque_Timeout(t *testing.T) {
	var n1 int
	handler := func(job *Job) error {
		n1++
		if n1 == 2 {
			time.Sleep(time.Millisecond * 20)
		}
		return nil
	}

	sc := slog.NewScavenger()
	tq := NewTickque(sc, WithMaxTickTime(time.Millisecond*5))
	for i := 0; i < 5; i++ {
		tq.AddJob("alpha", live.Nil)
	}
	if tq.Tick(10, handler) != 2 {
		t.Fatal(`tq.Tick(10, handler) != 2`)
	}
	if !sc.StringExists("a tickque tick timed out") {
		t.Fatal(`!sc.StringExists("a tickque tick timed out")`)
	}

	sc.Reset()
	if tq.Tick(10, handler) != 3 {
		t.Fatal(`tq.Tick(10, handler) != 3`)
	}
	if sc.Len() != 0 {
		t.Fatal(`sc.Len() != 0`)
	}
}
