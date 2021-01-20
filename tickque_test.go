package tickque

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
)

func TestTickque_Routine(t *testing.T) {
	var n int
	handler := func(job *Job) bool {
		n++
		return true
	}
	tq := NewTickque("alpha")
	for _, v := range []int{0, 3, 9, 10, 11, 19, 100} {
		n = 0
		for i := 0; i < v; i++ {
			tq.Enqueue(fmt.Sprintf("alpha-%d-%d", v, i), live.Nil)
		}
		var c1 int
		for tq.NumPendingJobs() > 0 {
			c3 := tq.NumPendingJobs()
			processed := tq.Tick(10, handler)
			if c2 := tq.NumPendingJobs(); v%10 == 0 || c3 >= 10 {
				if processed != 10 {
					t.Fatalf("processed != 10. v: %d, numPendingJobs: %d, processed: %d", v, c2, processed)
				}
			} else {
				if processed != v%10 {
					t.Fatalf("processed != int64(v) %% 10. v: %d, numPendingJobs: %d, processed: %d", v, c2, processed)
				}
			}
			c1++
		}
		if c1 != (v+9)/10 {
			t.Fatal("c1 != (v+9)/10")
		}
	}
}

func TestWithTickStartNtf(t *testing.T) {
	var n int
	handler := func(job *Job) bool {
		n++
		if n%11 == 1 {
			if job.Type != TickStart {
				t.Fatalf("job.Type != TickStart. job.Type: %s", job.Type)
			}
		} else {
			if job.Type == TickStart {
				t.Fatalf("job.Type == TickStart. job.Type: %s", job.Type)
			}
		}
		return true
	}
	tq := NewTickque("alpha", WithTickStartNtf())
	for _, v := range []int{0, 3, 9, 10, 11, 19, 100} {
		n = 0
		for i := 0; i < v; i++ {
			tq.Enqueue(fmt.Sprintf("alpha-%d-%d", v, i), live.Nil)
		}
		var c1 int
		for tq.NumPendingJobs() > 0 {
			c3 := tq.NumPendingJobs()
			processed := tq.Tick(10, handler)
			if c2 := tq.NumPendingJobs(); v%10 == 0 || c3 >= 10 {
				if processed != 10 {
					t.Fatalf("processed != 10. v: %d, numPendingJobs: %d, processed: %d", v, c2, processed)
				}
			} else {
				if processed != v%10 {
					t.Fatalf("processed != int64(v) %% 10. v: %d, numPendingJobs: %d, processed: %d", v, c2, processed)
				}
			}
			c1++
		}
		if c1 != (v+9)/10 {
			t.Fatal("c1 != (v+9)/10")
		}
	}
}

func TestTickque_Panic(t *testing.T) {
	var n int
	handler := func(job *Job) bool {
		if job.Type != fmt.Sprint(n) {
			t.Fatal("job.Type != fmt.Sprint(n)")
		}
		n++
		if n == 2 {
			panic("beta")
		}
		return true
	}
	scav := slog.NewScavenger()
	tq := NewTickque("alpha", WithLogger(scav))
	for i := 0; i < 5; i++ {
		tq.Enqueue(fmt.Sprint(i), live.Nil)
	}

	if processed := tq.Tick(10, handler); processed != 2 {
		t.Fatal("processed != 2")
	}
	if _, _, ok := scav.FindString("> panic:"); !ok {
		t.Fatal("panic not detected")
	}
	if tq.NumPendingJobs() != 3 {
		t.Fatal("tq.NumPendingJobs() != 3")
	}
	if tq.TotalProcessed() != 2 {
		t.Fatal("tq.TotalProcessed() != 2")
	}

	numLogs := scav.Len()
	if processed := tq.Tick(10, handler); processed != 3 {
		t.Fatal("processed != 3")
	}
	if numLogs != scav.Len() {
		t.Fatal("numLogs != scav.Len()")
	}
	if tq.NumPendingJobs() != 0 {
		t.Fatal("tq.NumPendingJobs() != 0")
	}
	if tq.TotalProcessed() != 5 {
		t.Fatal("tq.TotalProcessed() != 5")
	}
}

func TestTickque_Halt(t *testing.T) {
	var n int
	handler := func(job *Job) bool {
		n++
		return n != 2
	}
	tq := NewTickque("alpha")
	for i := 0; i < 15; i++ {
		tq.Enqueue(fmt.Sprint(i), live.Nil)
	}

	if processed := tq.Tick(10, handler); processed != 2 {
		t.Fatal("processed != 2")
	}
	if tq.NumPendingJobs() != 13 {
		t.Fatal("tq.NumPendingJobs() != 13")
	}
	if tq.TotalProcessed() != 2 {
		t.Fatal("tq.TotalProcessed() != 2")
	}

	if processed := tq.Tick(10, handler); processed != 10 {
		t.Fatal("processed != 10")
	}
	if tq.NumPendingJobs() != 3 {
		t.Fatal("tq.NumPendingJobs() != 3")
	}
	if tq.TotalProcessed() != 12 {
		t.Fatal("tq.TotalProcessed() != 12")
	}

	if processed := tq.Tick(10, handler); processed != 3 {
		t.Fatal("processed != 3")
	}
	if tq.NumPendingJobs() != 0 {
		t.Fatal("tq.NumPendingJobs() != 0")
	}
	if tq.TotalProcessed() != 15 {
		t.Fatal("tq.TotalProcessed() != 15")
	}
}

func TestWithTickExecTimeThreshold(t *testing.T) {
	var n int
	handler := func(job *Job) bool {
		if n++; n == 1 {
			time.Sleep(time.Millisecond * 30)
		}
		return true
	}

	scav := slog.NewScavenger()
	tq := NewTickque("alpha", WithLogger(scav), WithTickExecTimeThreshold(time.Millisecond*10))
	tq.Enqueue("1", live.Nil)
	tq.Enqueue("2", live.Nil)
	tq.Enqueue("3", live.Nil)

	if processed := tq.Tick(1, handler); processed != 1 {
		t.Fatal("processed != 1")
	}
	if _, _, ok := scav.FindString("the tick cost too much time"); !ok {
		t.Fatal("WithTickExecTimeThreshold does not work as expected")
	}

	scav.Reset()
	if processed := tq.Tick(1, handler); processed != 1 {
		t.Fatal("processed != 1")
	}
	if _, _, ok := scav.FindString("the tick cost too much time"); ok {
		t.Fatal("WithTickExecTimeThreshold does not work as expected")
	}
}

func TestTickque_Retry(t *testing.T) {
	var n int32
	tq := NewTickque("alpha")
	handler := func(job *Job) bool {
		switch job.Type {
		case "0":
			if n >= 0 {
				n++
				if job.TryNumber() != n {
					t.Fatal("job.TryNumber() != n")
				}
			}
		}
		tq.Retry(job)
		return true
	}

	if tq.Tick(1, handler) != 0 {
		t.Fatal("tq.Tick(1, handler) != 0")
	}

	tq.Enqueue("0", live.Nil)
	for i := 0; i < 10; i++ {
		tq.Tick(10, handler)
		if n != int32(i)+1 {
			t.Fatal("n != i+1")
		}
	}

	n = -1
	for i := 1; i < 10; i++ {
		tq.Enqueue(fmt.Sprint(i), live.Nil)
	}
	for i := 0; i < 50; i++ {
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
		tq.Enqueue(fmt.Sprint(i), live.Nil)
	}
	for i := 0; i < 50; i++ {
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
	handler := func(job *Job) bool {
		atomic.AddInt64(&counter, 1)
		if job.burstInfo.bool {
			atomic.AddInt64(&threadCounters[job.burstInfo.lane], 1)
		}
		n := rand.Intn(100)
		switch {
		case n < 5:
			atomic.AddInt64(&remaining, -1)
			atomic.AddInt64(&numPanics, 1)
			panic("boom!")
		case n < 20:
			atomic.AddInt64(&numRetries, 1)
			tq.Retry(job)
		default:
			atomic.AddInt64(&remaining, -1)
		}
		return true
	}

	const total = 100000
	tq = NewTickque("alpha", WithLogger(slog.DumbLogger{}), WithNumBurstThreads(numThreads))
	for i := 0; i < total; i++ {
		n := rand.Intn(100)
		switch {
		case n < 5:
			tq.Enqueue(fmt.Sprint(i), live.Nil)
		default:
			tq.EnqueueBurstJob(int64(i), fmt.Sprint(i), live.Nil)
		}
	}

	remaining = total
	var numProcessed int64
	for tq.NumPendingJobs() > 0 {
		n := tq.Tick(rand.Intn(300), handler)
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
}

func TestTickque_Shutdown(t *testing.T) {
	var n1 int
	handler1 := func(job *Job) bool {
		n1++
		return true
	}
	tq1 := NewTickque("alpha")
	liveHelper := live.NewHelper(nil)
	data := []int{0, 3, 9, 10, 11, 19, 100}
	for _, v := range data {
		tq1.Enqueue(fmt.Sprintf("alpha-%d", v), liveHelper.WrapInt(v))
	}
	if total, err := tq1.Shutdown(context.Background(), handler1); err != nil {
		t.Fatal(err)
	} else if total != len(data) {
		t.Fatal("total != len(data)", total)
	} else if n1 != len(data) {
		t.Fatal("n1 != len(data)")
	}

	var n2 int
	handler2 := func(job *Job) bool {
		n2++
		if v := job.Data.ToInt(); v == 10 {
			panic(v)
		}
		return true
	}
	tq2 := NewTickque("alpha", WithLogger(slog.DumbLogger{}))
	for _, v := range data {
		tq2.Enqueue(fmt.Sprintf("alpha-%d", v), liveHelper.WrapInt(v))
	}
	if total, err := tq2.Shutdown(context.Background(), handler2); err != nil {
		t.Fatal(err)
	} else if total != len(data) {
		t.Fatal("total != len(data)")
	} else if n2 != len(data) {
		t.Fatal("n2 != len(data)")
	}

	var n3 int
	handler3 := func(job *Job) bool {
		n3++
		if v := job.Data.ToInt(); v >= 10 {
			panic(v)
		}
		return true
	}
	tq3 := NewTickque("alpha", WithLogger(slog.DumbLogger{}))
	for _, v := range data {
		tq3.Enqueue(fmt.Sprintf("alpha-%d", v), liveHelper.WrapInt(v))
	}
	if total, err := tq3.Shutdown(context.Background(), handler3); err != nil {
		t.Fatal(err)
	} else if total != len(data) {
		t.Fatal("total != len(data)")
	} else if n3 != len(data) {
		t.Fatal("n3 != len(data)")
	}
}
