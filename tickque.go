package tickque

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
)

const (
	TickStart = "tickque:TickStart"
)

var (
	jobTickStart = &Job{Type: TickStart}
)

type Job struct {
	Type string
	Data live.Data

	tryNumber int
}

func (this *Job) TryNumber() int {
	return this.tryNumber
}

type JobHandler func(job *Job) bool

type Tickque struct {
	slog.Logger
	name                  string
	tickStartNtf          bool
	tickExecTimeThreshold time.Duration

	mu sync.Mutex
	dq *Deque

	totalProcessed int64
}

func NewTickque(name string, opts ...Option) (tq *Tickque) {
	tq = &Tickque{
		name:                  name,
		Logger:                slog.NewConsoleLogger(),
		tickExecTimeThreshold: time.Millisecond * 100,
		dq:                    NewDeque(),
	}
	for _, opt := range opts {
		opt(tq)
	}
	return
}

func minInt(n1, n2 int) int {
	if n1 < n2 {
		return n1
	} else {
		return n2
	}
}

func (this *Tickque) Tick(maxNumJobs int, jobHandler JobHandler) (numProcessed int) {
	startTime := time.Now()
	var pending []*Job
	var pendingIdx int
	defer func() {
		if r := recover(); r != nil {
			this.Errorf("<tickque.%s> panic: %+v\n%s", this.name, r, debug.Stack())
		}

		if pendingIdx < len(pending) {
			this.mu.Lock()
			for i := len(pending) - 1; i >= pendingIdx; i-- {
				this.dq.PushFront(pending[i])
			}
			this.mu.Unlock()
		}

		atomic.AddInt64(&this.totalProcessed, int64(numProcessed))
		if d := time.Since(startTime); d > this.tickExecTimeThreshold {
			this.Warnf("<tickque.%s> the tick cost too much time. d: %v", this.name, d)
		}
	}()

	if this.tickStartNtf {
		if !jobHandler(jobTickStart) {
			return
		}
	}

	this.mu.Lock()
	remainingJobs := minInt(this.dq.Len(), maxNumJobs)
	this.mu.Unlock()

	for remainingJobs > 0 {
		const batchSize = 16
		n := minInt(remainingJobs, batchSize)
		this.mu.Lock()
		pending = this.dq.DequeueMany(n)
		this.mu.Unlock()
		n = len(pending)
		for pendingIdx = 0; pendingIdx < n; {
			job := pending[pendingIdx]
			pendingIdx++
			numProcessed++
			if !jobHandler(job) {
				return
			}
		}
		if n < batchSize {
			return
		}
		remainingJobs -= n
	}
	return
}

func (this *Tickque) Enqueue(jobType string, jobData live.Data) {
	this.mu.Lock()
	this.dq.Enqueue(&Job{
		Type:      jobType,
		Data:      jobData,
		tryNumber: 1,
	})
	this.mu.Unlock()
}

func (this *Tickque) Retry(job *Job) {
	job.tryNumber++
	this.mu.Lock()
	this.dq.Enqueue(job)
	this.mu.Unlock()
}

func (this *Tickque) NumPendingJobs() int {
	this.mu.Lock()
	n := this.dq.Len()
	this.mu.Unlock()
	return n
}

func (this *Tickque) TotalProcessed() int64 {
	return atomic.LoadInt64(&this.totalProcessed)
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

func WithTickExecTimeThreshold(d time.Duration) Option {
	return func(tq *Tickque) {
		tq.tickExecTimeThreshold = d
	}
}
