package tickque

import (
	"sync"
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
