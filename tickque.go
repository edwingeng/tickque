package tickque

import (
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/edwingeng/deque"
	"github.com/edwingeng/slog"
)

const (
	BatchStart = "tickqueBatchStart"
	BatchEnd   = "tickqueBatchEnd"
)

var (
	batchStartJob = Job{Type: BatchStart}
	batchEndJob   = Job{Type: BatchEnd}
)

type Job struct {
	Type string
	Data []byte
}

func (_ Job) JobUUID() uint64 {
	return 0
}

func (j Job) JobType() string {
	return j.Type
}

func (j Job) JobData() []byte {
	return j.Data
}

type Tickque struct {
	slog.Logger
	name                    string
	jobHandler              func(job Job) int64
	maxJobsPerBatch         int64
	maxEnergyPointsPerBatch int64
	batchStart              bool
	batchEnd                bool

	mu sync.Mutex
	dq deque.Deque

	numProcessed int64
}

func NewTickque(name string, jobHandler func(job Job) int64, maxJobsPerBatch int64, opts ...Option) (tq *Tickque) {
	tq = &Tickque{
		name:                    name,
		Logger:                  slog.NewConsoleLogger(),
		jobHandler:              jobHandler,
		maxJobsPerBatch:         math.MaxInt64,
		maxEnergyPointsPerBatch: math.MaxInt64,
		dq:                      deque.NewDeque(),
	}
	if maxJobsPerBatch > 0 {
		tq.maxJobsPerBatch = maxJobsPerBatch
	}
	for _, opt := range opts {
		opt(tq)
	}
	return
}

func (this *Tickque) Process() {
	var pending []interface{}
	var idx int
	defer func() {
		if r := recover(); r != nil {
			this.Errorf("<tickque.%s> panic: %+v\n%s", this.name, r, debug.Stack())
			idx++
		}

		n := len(pending)
		if idx < n {
			this.mu.Lock()
			for i := n - 1; i >= idx; i-- {
				this.dq.PushFront(pending[i])
			}
			this.mu.Unlock()
		}
		if this.batchEnd {
			this.jobHandler(batchEndJob)
		}
	}()

	if this.batchStart {
		this.jobHandler(batchStartJob)
	}

	const batchSize = 16
	remainingJobs := this.maxJobsPerBatch
	remainingEnergyPoints := this.maxEnergyPointsPerBatch
	for remainingEnergyPoints > 0 && remainingJobs > 0 {
		var n int64 = batchSize
		if remainingJobs < batchSize {
			n = remainingJobs
		}
		pending := this.dq.DequeueMany(int(n))
		remainingJobs -= n
		for idx = 0; idx < len(pending) && remainingEnergyPoints > 0; idx++ {
			remainingEnergyPoints -= this.jobHandler(pending[idx].(Job))
		}
	}
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

func (this *Tickque) NumProcessed() int {
	return int(atomic.LoadInt64(&this.numProcessed))
}

type Option func(tq *Tickque)

func WithLogger(log slog.Logger) Option {
	return func(tq *Tickque) {
		tq.Logger = log
	}
}

func WithBatchStartNtf() Option {
	return func(tq *Tickque) {
		tq.batchStart = true
	}
}

func WithBatchEndNtf() Option {
	return func(tq *Tickque) {
		tq.batchEnd = true
	}
}

func WithMaxEnergyPointsPerBatch(n int64) Option {
	return func(tq *Tickque) {
		if n > 0 {
			tq.maxEnergyPointsPerBatch = n
		}
	}
}
