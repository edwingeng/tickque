package tickque

import (
	"fmt"
	"testing"

	"github.com/edwingeng/slog"
)

func TestTickque_Routine(t *testing.T) {
	var n int
	handler := func(job Job) bool {
		n++
		return true
	}
	tq := NewTickque("alpha", handler, 10)
	for _, v := range []int{0, 3, 9, 10, 11, 19, 100} {
		n = 0
		for i := 0; i < v; i++ {
			tq.Enqueue(fmt.Sprintf("alpha-%d-%d", v, i), nil)
		}
		var c1 int
		for tq.NumPendingJobs() > 0 {
			c3 := tq.NumPendingJobs()
			processed := tq.Tick()
			if c2 := tq.NumPendingJobs(); v%10 == 0 || c3 >= 10 {
				if processed != 10 {
					t.Fatalf("processed != 10. v: %d, numPendingJobs: %d, processed: %d", v, c2, processed)
				}
			} else {
				if processed != int64(v)%10 {
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

func TestWithBatchStartNtf(t *testing.T) {
	var n int
	handler := func(job Job) bool {
		n++
		if n%11 == 1 {
			if job.Type != BatchStart {
				t.Fatalf("job.Type != BatchStart. job.Type: %s", job.Type)
			}
		} else {
			if job.Type == BatchStart {
				t.Fatalf("job.Type == BatchStart. job.Type: %s", job.Type)
			}
		}
		return true
	}
	tq := NewTickque("alpha", handler, 10, WithBatchStartNtf())
	for _, v := range []int{0, 3, 9, 10, 11, 19, 100} {
		n = 0
		for i := 0; i < v; i++ {
			tq.Enqueue(fmt.Sprintf("alpha-%d-%d", v, i), nil)
		}
		var c1 int
		for tq.NumPendingJobs() > 0 {
			c3 := tq.NumPendingJobs()
			processed := tq.Tick()
			if c2 := tq.NumPendingJobs(); v%10 == 0 || c3 >= 10 {
				if processed != 10 {
					t.Fatalf("processed != 10. v: %d, numPendingJobs: %d, processed: %d", v, c2, processed)
				}
			} else {
				if processed != int64(v)%10 {
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
	handler := func(job Job) bool {
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
	tq := NewTickque("alpha", handler, 10, WithLogger(scav))
	for i := 0; i < 5; i++ {
		tq.Enqueue(fmt.Sprint(i), nil)
	}

	if processed := tq.Tick(); processed != 2 {
		t.Fatal("processed != 2")
	}
	if _, _, ok := scav.FindString("> panic:"); !ok {
		t.Fatal("panic not detected")
	}
	if tq.NumPendingJobs() != 3 {
		t.Fatal("tq.NumPendingJobs() != 3")
	}
	if tq.NumProcessed() != 2 {
		t.Fatal("tq.NumProcessed() != 2")
	}

	numLogs := scav.Len()
	if processed := tq.Tick(); processed != 3 {
		t.Fatal("processed != 3")
	}
	if numLogs != scav.Len() {
		t.Fatal("numLogs != scav.Len()")
	}
	if tq.NumPendingJobs() != 0 {
		t.Fatal("tq.NumPendingJobs() != 0")
	}
	if tq.NumProcessed() != 5 {
		t.Fatal("tq.NumProcessed() != 5")
	}
}

func TestTickque_Halt(t *testing.T) {
	var n int
	handler := func(job Job) bool {
		n++
		return n != 2
	}
	tq := NewTickque("alpha", handler, 10)
	for i := 0; i < 15; i++ {
		tq.Enqueue(fmt.Sprint(i), nil)
	}

	if processed := tq.Tick(); processed != 2 {
		t.Fatal("processed != 2")
	}
	if tq.NumPendingJobs() != 13 {
		t.Fatal("tq.NumPendingJobs() != 13")
	}
	if tq.NumProcessed() != 2 {
		t.Fatal("tq.NumProcessed() != 2")
	}

	if processed := tq.Tick(); processed != 10 {
		t.Fatal("processed != 10")
	}
	if tq.NumPendingJobs() != 3 {
		t.Fatal("tq.NumPendingJobs() != 3")
	}
	if tq.NumProcessed() != 12 {
		t.Fatal("tq.NumProcessed() != 12")
	}

	if processed := tq.Tick(); processed != 3 {
		t.Fatal("processed != 3")
	}
	if tq.NumPendingJobs() != 0 {
		t.Fatal("tq.NumPendingJobs() != 0")
	}
	if tq.NumProcessed() != 15 {
		t.Fatal("tq.NumProcessed() != 15")
	}
}
