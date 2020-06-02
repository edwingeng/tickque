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

	tryNumber int32
	burst     struct {
		bool
		int8
	}
}

func (this *Job) TryNumber() int32 {
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

	burst struct {
		numThreads uint64
		queues     []jobQueue
		wg         sync.WaitGroup
	}

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
	for i := 0; i < int(tq.burst.numThreads); i++ {
		tq.burst.queues = append(tq.burst.queues, newJobQueue())
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
	if this.tickStartNtf {
		if !jobHandler(jobTickStart) {
			return
		}
	}

	var total int64
	num := this.tickImpl(maxNumJobs, jobHandler, &this.jq)
	atomic.AddInt64(&total, int64(num))

	if d := maxNumJobs - num; d > 0 {
		for i := 0; i < len(this.burst.queues); i++ {
			jq := &this.burst.queues[i]
			jq.mu.Lock()
			empty := jq.dq.Empty()
			jq.mu.Unlock()
			if !empty {
				this.burst.wg.Add(1)
				go func() {
					num = this.tickImpl(d, jobHandler, jq)
					atomic.AddInt64(&total, int64(num))
					this.burst.wg.Done()
				}()
			}
		}
		this.burst.wg.Wait()
	}

	if d := time.Since(startTime); d > this.tickExecTimeThreshold {
		this.Warnf("<tickque.%s> the tick cost too much time. d: %v", this.name, d)
	}
	return int(atomic.LoadInt64(&total))
}

func (this *Tickque) tickImpl(maxNumJobs int, jobHandler JobHandler, jq *jobQueue) (numProcessed int) {
	var pending []*Job
	var pendingIdx int
	defer func() {
		if r := recover(); r != nil {
			this.Errorf("<tickque.%s> panic: %+v\n%s", this.name, r, debug.Stack())
		}

		if pendingIdx < len(pending) {
			jq.mu.Lock()
			for i := len(pending) - 1; i >= pendingIdx; i-- {
				jq.dq.PushFront(pending[i])
			}
			jq.mu.Unlock()
		}

		atomic.AddInt64(&this.totalProcessed, int64(numProcessed))
	}()

	jq.mu.Lock()
	remainingJobs := minInt(jq.dq.Len(), maxNumJobs)
	jq.mu.Unlock()

	for remainingJobs > 0 {
		const batchSize = 16
		n := minInt(remainingJobs, batchSize)
		jq.mu.Lock()
		pending = jq.dq.DequeueMany(n)
		jq.mu.Unlock()
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

func (this *Tickque) EnqueueBurstJob(hint int64, jobType string, jobData live.Data) {
	if this.burst.numThreads == 0 {
		panic("no WithNumBurstThreads option on creation")
	}

	index := int8(hash64(uint64(hint)) % this.burst.numThreads)
	jq := &this.burst.queues[index]
	jq.mu.Lock()
	jq.dq.Enqueue(&Job{
		Type:      jobType,
		Data:      jobData,
		tryNumber: 1,
		burst: struct {
			bool
			int8
		}{
			bool: true,
			int8: index,
		},
	})
	jq.mu.Unlock()
}

func hash64(x uint64) uint64 {
	x = (x ^ (x >> 30)) * uint64(0xbf58476d1ce4e5b9)
	x = (x ^ (x >> 27)) * uint64(0x94d049bb133111eb)
	return x ^ (x >> 31)
}

func (this *Tickque) Retry(job *Job) {
	job.tryNumber++
	if !job.burst.bool {
		this.jq.mu.Lock()
		this.jq.dq.Enqueue(job)
		this.jq.mu.Unlock()
	} else {
		jq := &this.burst.queues[job.burst.int8]
		jq.mu.Lock()
		jq.dq.Enqueue(job)
		jq.mu.Unlock()
	}
}

func (this *Tickque) NumPendingJobs() int {
	this.jq.mu.Lock()
	n := this.jq.dq.Len()
	this.jq.mu.Unlock()
	for i := 0; i < len(this.burst.queues); i++ {
		jq := &this.burst.queues[i]
		jq.mu.Lock()
		n += jq.dq.Len()
		jq.mu.Unlock()
	}
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

func WithNumBurstThreads(n int) Option {
	if n < 0 || n > 64 {
		panic("invalid number of burst threads")
	}
	return func(tq *Tickque) {
		tq.burst.numThreads = uint64(n)
	}
}
