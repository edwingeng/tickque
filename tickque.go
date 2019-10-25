package tickque

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwingeng/deque"
	"github.com/edwingeng/slog"
)

const (
	TickStart = "tickqueTickStart"
)

var (
	tickStartJob = &Job{Type: TickStart}
)

type Job struct {
	Type string
	Data []byte

	tryNumber int
}

func (this *Job) TryNumber() int {
	return this.tryNumber
}

type JobHandler func(job *Job) bool

type Tickque struct {
	slog.Logger
	name                       string
	tickStartNtf               bool
	tickExecutionTimeThreshold time.Duration

	mu sync.Mutex
	dq deque.Deque

	numProcessed int64
}

func NewTickque(name string, opts ...Option) (tq *Tickque) {
	tq = &Tickque{
		name:                       name,
		Logger:                     slog.NewConsoleLogger(),
		tickExecutionTimeThreshold: time.Millisecond * 100,
		dq:                         deque.NewDeque(),
	}
	for _, opt := range opts {
		opt(tq)
	}
	return
}

func (this *Tickque) Tick(maxNumJobs int64, jobHandler JobHandler) (numProcessed int64) {
	startTime := time.Now()
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
		if d := time.Since(startTime); d > this.tickExecutionTimeThreshold {
			this.Warnf("<tickque.%s> the tick cost too much time. d: %v", this.name, d)
		}
	}()

	if this.tickStartNtf {
		jobHandler(tickStartJob)
	}

	remainingJobs := maxNumJobs
	for remainingJobs > 0 {
		const batchSize = 16
		var n int64 = batchSize
		if remainingJobs < batchSize {
			n = remainingJobs
		}
		this.mu.Lock()
		pending = this.dq.DequeueMany(int(n))
		this.mu.Unlock()
		remainingJobs -= n
		for pendingIdx = 0; pendingIdx < len(pending); {
			job := pending[pendingIdx].(*Job)
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
	this.dq.Enqueue(&Job{Type: jobType, Data: jobData, tryNumber: 1})
	this.mu.Unlock()
}

func (this *Tickque) Retry(job *Job) {
	job.tryNumber++
	this.mu.Lock()
	this.dq.Enqueue(job)
	this.mu.Unlock()
}

func (this *Tickque) DequeueMany(max int) []*Job {
	this.mu.Lock()
	a := this.dq.DequeueMany(max)
	this.mu.Unlock()
	n := len(a)
	if n == 0 {
		return nil
	}
	b := make([]*Job, len(a), len(a))
	for i := 0; i < n; i++ {
		b[i] = a[i].(*Job)
	}
	return b
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

func WithTickStartNtf() Option {
	return func(tq *Tickque) {
		tq.tickStartNtf = true
	}
}

func WithTickExecutionTimeThreshold(d time.Duration) Option {
	return func(tq *Tickque) {
		tq.tickExecutionTimeThreshold = d
	}
}
