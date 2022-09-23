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

var (
	ErrBreak = errors.New("break")
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

func (j *Job) TryNumber() int32 {
	return j.tryNumber
}

func (j *Job) Bursting() bool {
	return j.burstInfo.bool
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
		Logger:               slog.NewDevelopmentConfig().MustBuild(),
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

func (tq *Tickque) invokeJobHandler(jobHandler JobHandler, job *Job) bool {
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("<tickque.%s> jobType: %s, panic: %+v\n%s",
				tq.name, job.Type, r, debug.Stack())
			tq.Errorw(errMsg, "jobData", fmt.Sprint(job.Data.Value()))
		}
	}()
	if err := jobHandler(job); err != nil {
		if err == ErrBreak {
			return false
		}
		tq.Errorf("tickque job failed. jobType: %s, err: %s", job.Type, err)
	}
	return true
}

func (tq *Tickque) Tick(maxJobs int, jobHandler JobHandler) int {
	var total int64
	startTime := time.Now()
	n1 := tq.processJobQueue(maxJobs, jobHandler, &tq.jq)
	atomic.AddInt64(&total, int64(n1))

	if d := maxJobs - n1; d > 0 {
		for i := 0; i < len(tq.burst.queues); i++ {
			jq := &tq.burst.queues[i]
			jq.mu.Lock()
			empty := jq.dq.IsEmpty()
			jq.mu.Unlock()
			if !empty {
				tq.burst.wg.Add(1)
				go func() {
					n2 := tq.processJobQueue(d, jobHandler, jq)
					atomic.AddInt64(&total, int64(n2))
					tq.burst.wg.Done()
				}()
			}
		}
		tq.burst.wg.Wait()
	}

	if d := time.Since(startTime); d > tq.slowWarningThreshold {
		tq.Warnf("<tickque.%s> the tick cost too much time. d: %v", tq.name, d)
	}

	return int(atomic.LoadInt64(&total))
}

func (tq *Tickque) processJobQueue(maxJobs int, jobHandler JobHandler, jq *jobQueue) (numProcessed int) {
	var pending []*Job
	var pendingIdx int
	defer func() {
		if r := recover(); r != nil {
			tq.Errorf("<tickque.%s> panic: %+v\n%s", tq.name, r, debug.Stack())
		}

		if pendingIdx < len(pending) {
			jq.mu.Lock()
			for i := len(pending) - 1; i >= pendingIdx; i-- {
				jq.dq.PushFront(pending[i])
			}
			jq.mu.Unlock()
		}

		atomic.AddInt64(&tq.totalProcessed, int64(numProcessed))
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
			if !tq.invokeJobHandler(jobHandler, job) {
				return
			}
		}
		remainingJobs -= n2
	}
	return
}

func (tq *Tickque) AddJob(jobType string, jobData live.Data) {
	j := jobPool.Get().(*Job)
	j.Type = jobType
	j.Data = jobData
	j.tryNumber = 1
	j.burstInfo = burstInfo{}

	tq.jq.mu.Lock()
	tq.jq.dq.Enqueue(j)
	tq.jq.mu.Unlock()
}

func (tq *Tickque) AddBurstJob(hint int64, jobType string, jobData live.Data) {
	if tq.burst.numThreads == 0 {
		panic("it seems that the WithNumBurstThreads option was forgotten")
	}

	index := int8(hash64(uint64(hint)) % tq.burst.numThreads)
	jq := &tq.burst.queues[index]

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

func (tq *Tickque) Postpone(job *Job) {
	job.tryNumber++
	if !job.burstInfo.bool {
		tq.jq.mu.Lock()
		tq.jq.dq.Enqueue(job)
		tq.jq.mu.Unlock()
	} else {
		jq := &tq.burst.queues[job.burstInfo.lane]
		jq.mu.Lock()
		jq.dq.Enqueue(job)
		jq.mu.Unlock()
	}
}

func (tq *Tickque) NumPendingJobs() int {
	tq.jq.mu.Lock()
	n := tq.jq.dq.Len()
	tq.jq.mu.Unlock()
	for i := 0; i < len(tq.burst.queues); i++ {
		jq := &tq.burst.queues[i]
		jq.mu.Lock()
		n += jq.dq.Len()
		jq.mu.Unlock()
	}
	return n
}

func (tq *Tickque) TotalProcessed() int64 {
	return atomic.LoadInt64(&tq.totalProcessed)
}

func (tq *Tickque) Shutdown(ctx context.Context, jobHandler JobHandler) (int, error) {
	var total int
	var round int
	for {
		c := tq.NumPendingJobs()
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
			total += tq.Tick(maxJobs, jobHandler)
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
