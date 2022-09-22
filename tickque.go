package tickque

import (
	"context"
	"errors"
	"fmt"
	"github.com/edwingeng/deque/v2"
	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TickStart = "tickque:TickStart"
)

var (
	ErrBreak = errors.New("break")
)

var (
	jobTickStart = &Job{Type: TickStart}
)

type burstInfo struct {
	bool
	lane int8
}

type Job struct {
	Type string
	Data live.Data

	tryNumber int32
	burstInfo burstInfo
}

func (this *Job) TryNumber() int32 {
	return this.tryNumber
}

func (this *Job) Bursting() bool {
	return this.burstInfo.bool
}

type JobHandler func(job *Job) error

type jobQueue struct {
	mu sync.Mutex
	dq deque.Deque[*Job]
}

func newJobQueue() jobQueue {
	return jobQueue{dq: *deque.NewDeque[*Job]()}
}

type Tickque struct {
	slog.Logger
	name                 string
	tickStartNtf         bool
	slowWarningThreshold time.Duration

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
		name:                 name,
		Logger:               slog.NewConsoleLogger(),
		slowWarningThreshold: time.Millisecond * 100,
		jq:                   newJobQueue(),
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

func (this *Tickque) invokeJobHandler(jobHandler JobHandler, job *Job) bool {
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("<tickque.%s> jobType: %s, panic: %+v\n%s",
				this.name, job.Type, r, debug.Stack())
			this.Errorw(errMsg, "jobData", fmt.Sprint(job.Data.V()))
		}
	}()
	if err := jobHandler(job); err != nil {
		if err == ErrBreak {
			return false
		}
		this.Errorf("tickque job failed. jobType: %s, err: %s", job.Type, err)
	}
	return true
}

func (this *Tickque) Tick(maxJobs int, jobHandler JobHandler) int {
	startTime := time.Now()
	if this.tickStartNtf {
		if !this.invokeJobHandler(jobHandler, jobTickStart) {
			return 0
		}
	}

	var total int64
	n1 := this.processJobQueue(maxJobs, jobHandler, &this.jq)
	atomic.AddInt64(&total, int64(n1))

	if d := maxJobs - n1; d > 0 {
		for i := 0; i < len(this.burst.queues); i++ {
			jq := &this.burst.queues[i]
			jq.mu.Lock()
			empty := jq.dq.IsEmpty()
			jq.mu.Unlock()
			if !empty {
				this.burst.wg.Add(1)
				go func() {
					n2 := this.processJobQueue(d, jobHandler, jq)
					atomic.AddInt64(&total, int64(n2))
					this.burst.wg.Done()
				}()
			}
		}
		this.burst.wg.Wait()
	}

	if d := time.Since(startTime); d > this.slowWarningThreshold {
		this.Warnf("<tickque.%s> the tick cost too much time. d: %v", this.name, d)
	}

	return int(atomic.LoadInt64(&total))
}

func (this *Tickque) processJobQueue(maxJobs int, jobHandler JobHandler, jq *jobQueue) (numProcessed int) {
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
	remainingJobs := minInt(jq.dq.Len(), maxJobs)
	jq.mu.Unlock()

	for remainingJobs > 0 {
		const batchSize = 16
		n1 := minInt(remainingJobs, batchSize)
		jq.mu.Lock()
		pending = jq.dq.DequeueMany(n1)
		jq.mu.Unlock()
		n2 := len(pending)
		for pendingIdx = 0; pendingIdx < n2; {
			job := pending[pendingIdx]
			pendingIdx++
			numProcessed++
			if !this.invokeJobHandler(jobHandler, job) {
				return
			}
		}
		remainingJobs -= n2
	}
	return
}

func (this *Tickque) AddJob(jobType string, jobData live.Data) {
	j := jobPool.Get().(*Job)
	j.Type = jobType
	j.Data = jobData
	j.tryNumber = 1
	j.burstInfo = burstInfo{}

	this.jq.mu.Lock()
	this.jq.dq.Enqueue(j)
	this.jq.mu.Unlock()
}

func (this *Tickque) AddBurstJob(hint int64, jobType string, jobData live.Data) {
	if this.burst.numThreads == 0 {
		panic("it seems that the WithNumBurstThreads option was forgotten")
	}

	index := int8(hash64(uint64(hint)) % this.burst.numThreads)
	jq := &this.burst.queues[index]

	j := jobPool.Get().(*Job)
	j.Type = jobType
	j.Data = jobData
	j.tryNumber = 1
	j.burstInfo.bool = true
	j.burstInfo.lane = index

	jq.mu.Lock()
	jq.dq.Enqueue(j)
	jq.mu.Unlock()
}

func hash64(x uint64) uint64 {
	x = (x ^ (x >> 30)) * uint64(0xbf58476d1ce4e5b9)
	x = (x ^ (x >> 27)) * uint64(0x94d049bb133111eb)
	return x ^ (x >> 31)
}

func (this *Tickque) Postpone(job *Job) {
	job.tryNumber++
	if !job.burstInfo.bool {
		this.jq.mu.Lock()
		this.jq.dq.Enqueue(job)
		this.jq.mu.Unlock()
	} else {
		jq := &this.burst.queues[job.burstInfo.lane]
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

func (this *Tickque) Shutdown(ctx context.Context, jobHandler JobHandler) (int, error) {
	var total int
	var round int
	for {
		c := this.NumPendingJobs()
		if c == 0 {
			break
		}
		round++
		if round > 1 {
			const nap = time.Millisecond * 20
			time.Sleep(nap)
		}
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
			maxJobs := 1000
			if c < maxJobs {
				maxJobs = c
			}
			total += this.Tick(maxJobs, jobHandler)
		}
	}
	return total, nil
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

func WithSlowWarningThreshold(d time.Duration) Option {
	return func(tq *Tickque) {
		tq.slowWarningThreshold = d
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
