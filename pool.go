package tickque

import (
	"sync"

	"github.com/edwingeng/live"
)

var (
	jobPool = sync.Pool{
		New: func() interface{} {
			return &Job{}
		},
	}
)

func Recycle(job *Job) {
	jobPool.Put(job)
}

func NewJobFromPool(jobType string, jobData live.Data, tryNumber int32) *Job {
	j := jobPool.Get().(*Job)
	j.Type = jobType
	j.Data = jobData
	j.tryNumber = tryNumber
	j.burstInfo = burstInfo{}
	return j
}
