package tickque

import (
	"github.com/edwingeng/live"
)

type Job struct {
	Type string
	Data live.Data

	hint        int64
	retryNumber int32
}

func (j *Job) Hint() int64 {
	return j.hint
}

func (j *Job) RetryNumber() int32 {
	return j.retryNumber
}
