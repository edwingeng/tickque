package tickque

import (
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/edwingeng/deque"
	"github.com/edwingeng/slog"
)

const (
	BatchStart = "tickqueBatchStart"
)

var (
	batchStartJob = Job{Type: BatchStart}
)

type Job struct {
	Type string
	Data []byte
}

type JobHandler func(job Job) bool

type Tickque struct {
	slog.Logger
	name          string
	batchStartNtf bool

	mu sync.Mutex
	dq deque.Deque

	numProcessed int64
}

func NewTickque(name string, opts ...Option) (tq *Tickque) {
	tq = &Tickque{
		name:   name,
		Logger: slog.NewConsoleLogger(),
		dq:     deque.NewDeque(),
	}
	for _, opt := range opts {
		opt(tq)
	}
	return
}

func (this *Tickque) Tick(maxNumJobs int64, jobHandler JobHandler) (numProcessed int64) {
	var pending []interface{}
	var pendingIdx int
	defer func() {
		if r := recover(); r != nil {
			this.Errorf("<tickque.%s> panic: %+v\n%s", this.name, r, debug.Stack())
		}

		n := len(pending)
		if pendingIdx < n {
			this.mu.Lock()
			for i := n - 1; i >= pendingIdx; i-- {
				this.dq.PushFront(pending[i])
			}
			this.mu.Unlock()
		}
		atomic.AddInt64(&this.numProcessed, numProcessed)
	}()

	if this.batchStartNtf {
		jobHandler(batchStartJob)
	}

	remainingJobs := maxNumJobs
	for remainingJobs > 0 {
		const batchSize = 16
		var n int64 = batchSize
		if remainingJobs < batchSize {
			n = remainingJobs
		}
		pending = this.dq.DequeueMany(int(n))
		remainingJobs -= n
		for pendingIdx = 0; pendingIdx < len(pending); {
			job := pending[pendingIdx].(Job)
			pendingIdx++
			numProcessed++
			if !jobHandler(job) {
				return
			}
		}
		if len(pending) < int(n) {
			return
		}
	}
	return
}

func (this *Tickque) Enqueue(jobType string, jobData []byte) {
	this.mu.Lock()
	this.dq.Enqueue(Job{Type: jobType, Data: jobData})
	this.mu.Unlock()
}

func (this *Tickque) NumPendingJobs() int {
	this.mu.Lock()
	n := this.dq.Len()
	this.mu.Unlock()
	return n
}

func (this *Tickque) NumProcessed() int64 {
	return atomic.LoadInt64(&this.numProcessed)
}

type Option func(tq *Tickque)

func WithLogger(log slog.Logger) Option {
	return func(tq *Tickque) {
		tq.Logger = log
	}
}

func WithBatchStartNtf() Option {
	return func(tq *Tickque) {
		tq.batchStartNtf = true
	}
}
