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

type jobQueue struct {
	mu sync.Mutex
	dq Deque
}

func newJobQueue() jobQueue {
	return jobQueue{dq: *NewDeque()}
}

type Tickque struct {
	slog.Logger
	name                  string
	tickStartNtf          bool
	tickExecTimeThreshold time.Duration

	jq jobQueue

	totalProcessed int64
}

func NewTickque(name string, opts ...Option) (tq *Tickque) {
	tq = &Tickque{
		name:                  name,
		Logger:                slog.NewConsoleLogger(),
		tickExecTimeThreshold: time.Millisecond * 100,
		jq:                    newJobQueue(),
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
			this.jq.mu.Lock()
			for i := len(pending) - 1; i >= pendingIdx; i-- {
				this.jq.dq.PushFront(pending[i])
			}
			this.jq.mu.Unlock()
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

	this.jq.mu.Lock()
	remainingJobs := minInt(this.jq.dq.Len(), maxNumJobs)
	this.jq.mu.Unlock()

	for remainingJobs > 0 {
		const batchSize = 16
		n := minInt(remainingJobs, batchSize)
		this.jq.mu.Lock()
		pending = this.jq.dq.DequeueMany(n)
		this.jq.mu.Unlock()
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
	this.jq.mu.Lock()
	this.jq.dq.Enqueue(&Job{
		Type:      jobType,
		Data:      jobData,
		tryNumber: 1,
	})
	this.jq.mu.Unlock()
}

func (this *Tickque) Retry(job *Job) {
	job.tryNumber++
	this.jq.mu.Lock()
	this.jq.dq.Enqueue(job)
	this.jq.mu.Unlock()
}

func (this *Tickque) NumPendingJobs() int {
	this.jq.mu.Lock()
	n := this.jq.dq.Len()
	this.jq.mu.Unlock()
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
