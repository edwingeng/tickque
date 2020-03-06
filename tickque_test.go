package tickque

import (
	"fmt"
	"testing"
	"time"

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
			tq.Enqueue(fmt.Sprintf("alpha-%d-%d", v, i), nil)
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
			tq.Enqueue(fmt.Sprintf("alpha-%d-%d", v, i), nil)
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
		tq.Enqueue(fmt.Sprint(i), nil)
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
		tq.Enqueue(fmt.Sprint(i), nil)
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

func TestTickque_DequeueMany(t *testing.T) {
	tq := NewTickque("alpha")
	for i := 0; i < 20; i++ {
		tq.Enqueue(fmt.Sprint(i), nil)
	}
	for i := 1; i < 7; i++ {
		jobs := tq.DequeueMany(i)
		switch i {
		case 6:
			if len(jobs) != 5 {
				t.Fatal(len(jobs) != 5)
			}
		default:
			if len(jobs) != i {
				t.Fatalf("len(jobs) != i. i: %d", i)
			}
		}
		for _, j := range jobs {
			if j.Type == "" {
				t.Fatal(`j.Type == ""`)
			}
		}
	}
}

func TestWithTickExecutionTimeThreshold(t *testing.T) {
	var n int
	handler := func(job *Job) bool {
		if n++; n == 1 {
			time.Sleep(time.Millisecond * 30)
		}
		return true
	}

	scav := slog.NewScavenger()
	tq := NewTickque("alpha", WithLogger(scav), WithTickExecTimeThreshold(time.Millisecond*10))
	tq.Enqueue("1", nil)
	tq.Enqueue("2", nil)
	tq.Enqueue("3", nil)

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
	var n int
	tq := NewTickque("alpha")
	handler := func(job *Job) bool {
		n++
		if job.TryNumber() != n {
			t.Fatal("job.TryNumber() != n")
		}
		tq.Retry(job)
		return true
	}

	tq.Enqueue("1", nil)
	for i := 0; i < 10; i++ {
		tq.Tick(10, handler)
	}
}
