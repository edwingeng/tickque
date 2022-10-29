package tickque

import (
	"context"
	"fmt"
	"github.com/edwingeng/deque/v2"
	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
	"go.uber.org/atomic"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

type JobHandler func(job *Job) error

type jobQueue struct {
	sync.Mutex
	dq deque.Deque[*Job]
}

func newJobQueue() jobQueue {
	return jobQueue{dq: *deque.NewDeque[*Job]()}
}

type Tickque struct {
	slog.Logger
	maxTickTime time.Duration

	burst struct {
		nq     uint64
		queues []jobQueue
		wg     sync.WaitGroup
	}

	main jobQueue
}

func NewTickque(logger slog.Logger, opts ...Option) (tq *Tickque) {
	tq = &Tickque{
		maxTickTime: time.Millisecond * 50,
		main:        newJobQueue(),
	}
	if logger != nil {
		tq.Logger = logger
	} else {
		tq.Logger = slog.NewDevelopmentConfig().MustBuild()
	}
	for _, opt := range opts {
		opt(tq)
	}
	for i := 0; i < int(tq.burst.nq); i++ {
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

func (tq *Tickque) invokeJobHandler(jobHandler JobHandler, job *Job) {
	defer func() {
		if r := recover(); r != nil {
			str := fmt.Sprintf("<tickque> jobType: %s, panic: %+v\n%s", job.Type, r, debug.Stack())
			tq.Errorw(str, "jobData", job.Data.Value())
		}
	}()
	if err := jobHandler(job); err != nil {
		tq.Errorw(fmt.Sprintf("a tickque job failed. jobType: %s", job.Type), "err", err)
	}
}

func (tq *Tickque) Tick(maxJobs int, jobHandler JobHandler) int {
	if maxJobs <= 0 {
		panic("maxJobs must be a positive number")
	}

	var total atomic.Int64
	startTime := time.Now()
	count := tq.processJobQueue(&tq.main, maxJobs, jobHandler, startTime)
	total.Add(count)

	if tq.burst.nq > 0 {
		if d := maxJobs - int(count); d > 0 {
			for i := 0; i < len(tq.burst.queues); i++ {
				tq.processBurstQueue(&tq.burst.queues[i], d, jobHandler, startTime, &total)
			}
			tq.burst.wg.Wait()
		}
	}

	return int(total.Load())
}

func (tq *Tickque) processBurstQueue(jq *jobQueue, maxJobs int, jobHandler JobHandler, startTime time.Time, total *atomic.Int64) {
	jq.Lock()
	isEmpty := jq.dq.IsEmpty()
	jq.Unlock()
	if isEmpty {
		return
	}

	tq.burst.wg.Add(1)
	go func() {
		defer tq.burst.wg.Done()
		count := tq.processJobQueue(jq, maxJobs, jobHandler, startTime)
		total.Add(count)
	}()
}

func (tq *Tickque) processJobQueue(jq *jobQueue, maxJobs int, jobHandler JobHandler, startTime time.Time) int64 {
	const batchSize = 16
	var pitch [batchSize]*Job
	var pending = pitch[:0]
	var idx int
	defer func() {
		if r := recover(); r != nil {
			tq.Errorf("<tickque> panic: %+v\n%s", r, debug.Stack())
		}

		if idx < len(pending) {
			jq.Lock()
			for i := len(pending) - 1; i >= idx; i-- {
				jq.dq.PushFront(pending[i])
			}
			jq.Unlock()
		}
	}()

	jq.Lock()
	remaining := minInt(jq.dq.Len(), maxJobs)
	jq.Unlock()

	var count int64
	for remaining > 0 {
		n1 := minInt(remaining, batchSize)
		jq.Lock()
		pending = jq.dq.DequeueManyWithBuffer(n1, pending)
		jq.Unlock()
		n2 := len(pending)
		var job *Job
		for idx = 0; idx < n2; {
			job = pending[idx]
			idx++
			count++
			tq.invokeJobHandler(jobHandler, job)
			if dt := time.Since(startTime); dt > tq.maxTickTime {
				tq.Warnf("a tickque tick timed out. elapsed: %v", dt)
				return count
			}
		}
		remaining -= n2
	}

	return count
}

func (tq *Tickque) AddJob(jobType string, jobData live.Data) {
	job := jobPool.Get().(*Job)
	job.Type = jobType
	job.Data = jobData
	job.hint = 0
	job.retryNumber = 0

	tq.main.Lock()
	tq.main.dq.Enqueue(job)
	tq.main.Unlock()
}

func (tq *Tickque) AddBurstJob(hint int64, jobType string, jobData live.Data) {
	if tq.burst.nq == 0 {
		panic("WithNumBurstThreads was absent when creating the Tickque instance")
	}
	if hint == 0 {
		panic("hint cannot be zero")
	}

	job := jobPool.Get().(*Job)
	job.Type = jobType
	job.Data = jobData
	job.hint = hint
	job.retryNumber = 0

	slot := scatter(uint64(hint)) % (tq.burst.nq)
	jq := &tq.burst.queues[slot]
	jq.Lock()
	jq.dq.Enqueue(job)
	jq.Unlock()
}

func scatter(v uint64) uint64 {
	v = (v ^ (v >> 30)) * uint64(0xbf58476d1ce4e5b9)
	v = (v ^ (v >> 27)) * uint64(0x94d049bb133111eb)
	return v ^ (v >> 31)
}

func (tq *Tickque) Postpone(job *Job) {
	job.retryNumber++
	if job.hint == 0 {
		tq.main.Lock()
		tq.main.dq.Enqueue(job)
		tq.main.Unlock()
	} else {
		slot := scatter(uint64(job.hint)) % (tq.burst.nq)
		jq := &tq.burst.queues[slot]
		jq.Lock()
		jq.dq.Enqueue(job)
		jq.Unlock()
	}
}

func (tq *Tickque) NumPendingJobs() int {
	tq.main.Lock()
	n := tq.main.dq.Len()
	tq.main.Unlock()
	for i := 0; i < len(tq.burst.queues); i++ {
		jq := &tq.burst.queues[i]
		jq.Lock()
		n += jq.dq.Len()
		jq.Unlock()
	}
	return n
}

func (tq *Tickque) Shutdown(ctx context.Context, jobHandler JobHandler) (int, error) {
	deadline, ok := ctx.Deadline()
	if ok {
		tmp := tq.maxTickTime
		tq.maxTickTime = deadline.Sub(time.Now())
		defer func() {
			tq.maxTickTime = tmp
		}()
	}

	var total int
	for i := 0; ; i++ {
		runtime.Gosched()
		c := tq.NumPendingJobs()
		if c == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
			total += tq.Tick(100, jobHandler)
		}
	}
	return total, nil
}

type Option func(tq *Tickque)

func WithMaxTickTime(d time.Duration) Option {
	return func(tq *Tickque) {
		tq.maxTickTime = d
	}
}

func WithNumBurstThreads(n int) Option {
	if n < 0 || n > 64 {
		panic("invalid number of burst threads")
	}
	return func(tq *Tickque) {
		tq.burst.nq = uint64(n)
	}
}
