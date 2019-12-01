package tickque

import (
	"sync/atomic"
)

const chunkSize = 255

var elemDefValue Elem

type chunk struct {
	data [chunkSize]Elem
	s    int
	e    int
}

func (c *chunk) back() Elem {
	if c.e > c.s {
		return c.data[c.e-1]
	}
	return elemDefValue
}

func (c *chunk) front() Elem {
	if c.e > c.s {
		return c.data[c.s]
	}
	return elemDefValue
}

type Deque struct {
	chunks   []*chunk
	ptrPitch []*chunk
	sFree    int
	eFree    int

	chunkPool *chunkPool
}

var (
	sharedChunkPool = newChunkPool(func() interface{} {
		return &chunk{}
	})
)

// NewDeque creates a new Deque.
func NewDeque() *Deque {
	dq := &Deque{
		ptrPitch:  make([]*chunk, 64),
		sFree:     32,
		eFree:     32,
		chunkPool: sharedChunkPool,
	}
	return dq
}

func (dq *Deque) realloc() {
	newPitchLen := len(dq.ptrPitch) * 2
	newPitch := make([]*chunk, newPitchLen)
	n := len(dq.chunks)
	dq.sFree = newPitchLen/2 - n/2
	dq.eFree = newPitchLen - dq.sFree - n
	newChunks := newPitch[dq.sFree : dq.sFree+n]
	for i := 0; i < n; i++ {
		newChunks[i] = dq.chunks[i]
	}
	dq.ptrPitch = newPitch
	dq.chunks = newChunks
}

func (dq *Deque) expandEnd() {
	if dq.eFree == 0 {
		dq.realloc()
	}
	c := dq.chunkPool.Get().(*chunk)
	c.s, c.e = 0, 0
	dq.eFree--
	newEnd := len(dq.ptrPitch) - dq.eFree
	dq.ptrPitch[newEnd-1] = c
	dq.chunks = dq.ptrPitch[dq.sFree:newEnd]
}

func (dq *Deque) expandStart() {
	if dq.sFree == 0 {
		dq.realloc()
	}
	c := dq.chunkPool.Get().(*chunk)
	c.s, c.e = chunkSize, chunkSize
	dq.sFree--
	dq.ptrPitch[dq.sFree] = c
	newEnd := len(dq.ptrPitch) - dq.eFree
	dq.chunks = dq.ptrPitch[dq.sFree:newEnd]
}

func (dq *Deque) shrinkEnd() {
	n := len(dq.ptrPitch)
	if dq.sFree+dq.eFree == n {
		return
	}
	newEnd := n - dq.eFree - 1
	c := dq.ptrPitch[newEnd]
	dq.ptrPitch[newEnd] = nil
	dq.chunkPool.Put(c)
	dq.eFree++
	dq.chunks = dq.ptrPitch[dq.sFree:newEnd]
	if dq.sFree+dq.eFree == n {
		dq.sFree = n / 2
		dq.eFree = n - dq.sFree
		return
	}
}

func (dq *Deque) shrinkStart() {
	n := len(dq.ptrPitch)
	if dq.sFree+dq.eFree == n {
		return
	}
	c := dq.ptrPitch[dq.sFree]
	dq.ptrPitch[dq.sFree] = nil
	dq.chunkPool.Put(c)
	dq.sFree++
	newEnd := len(dq.ptrPitch) - dq.eFree
	dq.chunks = dq.ptrPitch[dq.sFree:newEnd]
	if dq.sFree+dq.eFree == n {
		dq.sFree = n / 2
		dq.eFree = n - dq.sFree
		return
	}
}

func (dq *Deque) PushBack(v Elem) {
	var c *chunk
	n := len(dq.chunks)
	if n == 0 {
		dq.expandEnd()
		c = dq.chunks[n]
	} else {
		c = dq.chunks[n-1]
		if c.e == chunkSize {
			dq.expandEnd()
			c = dq.chunks[n]
		}
	}
	c.data[c.e] = v
	c.e++
}

func (dq *Deque) PushFront(v Elem) {
	var c *chunk
	n := len(dq.chunks)
	if n == 0 {
		dq.expandStart()
		c = dq.chunks[0]
	} else {
		c = dq.chunks[0]
		if c.s == 0 {
			dq.expandStart()
			c = dq.chunks[0]
		}
	}
	c.s--
	c.data[c.s] = v
}

func (dq *Deque) PopBack() Elem {
	n := len(dq.chunks)
	if n == 0 {
		return elemDefValue
	}
	c := dq.chunks[n-1]
	if c.e == c.s {
		return elemDefValue
	}
	c.e--
	r := c.data[c.e]
	c.data[c.e] = elemDefValue
	if c.e == 0 {
		dq.shrinkEnd()
	}
	return r
}

func (dq *Deque) PopManyBack(max int) []Elem {
	n := dq.Len()
	if n == 0 {
		return nil
	}
	if max > 0 && n > max {
		n = max
	}
	vals := make([]Elem, n)
	x := len(dq.chunks) - 1
	for i := 0; i < n; i++ {
		c := dq.chunks[x]
		c.e--
		vals[i] = c.data[c.e]
		c.data[c.e] = elemDefValue
		if c.e == 0 {
			dq.shrinkEnd()
			x--
		}
	}
	return vals
}

func (dq *Deque) PopFront() Elem {
	n := len(dq.chunks)
	if n == 0 {
		return elemDefValue
	}
	c := dq.chunks[0]
	if c.e == c.s {
		return elemDefValue
	}
	r := c.data[c.s]
	c.data[c.s] = elemDefValue
	c.s++
	if c.s == chunkSize {
		dq.shrinkStart()
	}
	return r
}

func (dq *Deque) PopManyFront(max int) []Elem {
	n := dq.Len()
	if n == 0 {
		return nil
	}
	if max > 0 && n > max {
		n = max
	}
	vals := make([]Elem, n)
	for i := 0; i < n; i++ {
		c := dq.chunks[0]
		vals[i] = c.data[c.s]
		c.data[c.s] = elemDefValue
		c.s++
		if c.s == chunkSize {
			dq.shrinkStart()
		}
	}
	return vals
}

func (dq *Deque) Back() Elem {
	n := len(dq.chunks)
	if n == 0 {
		return elemDefValue
	}
	return dq.chunks[n-1].back()
}

func (dq *Deque) Front() Elem {
	n := len(dq.chunks)
	if n == 0 {
		return elemDefValue
	}
	return dq.chunks[0].front()
}

func (dq *Deque) Empty() bool {
	n := len(dq.chunks)
	return n == 0 || n == 1 && dq.chunks[0].e == dq.chunks[0].s
}

func (dq *Deque) Len() int {
	n := len(dq.chunks)
	switch n {
	case 0:
		return 0
	case 1:
		return dq.chunks[0].e - dq.chunks[0].s
	default:
		return chunkSize - dq.chunks[0].s + dq.chunks[n-1].e + (n-2)*chunkSize
	}
}

func (dq *Deque) Enqueue(v Elem) {
	dq.PushBack(v)
}

func (dq *Deque) Dequeue() Elem {
	return dq.PopFront()
}

func (dq *Deque) DequeueMany(max int) []Elem {
	return dq.PopManyFront(max)
}

func (dq *Deque) Dump() []Elem {
	n := dq.Len()
	if n == 0 {
		return nil
	}

	vals := make([]Elem, n)
	var idx int
	for _, c := range dq.chunks {
		for i := c.s; i < c.e; i++ {
			vals[idx] = c.data[i]
			idx++
		}
	}
	return vals
}

func (dq *Deque) Range(f func(v Elem) bool) {
	n := dq.Len()
	if n == 0 {
		return
	}

	for _, c := range dq.chunks {
		for i := c.s; i < c.e; i++ {
			if !f(c.data[i]) {
				return
			}
		}
	}
}

// NumChunksAllocated returns the number of chunks allocated by now.
func NumChunksAllocated() int64 {
	return atomic.LoadInt64(&sharedChunkPool.numChunksAllocated)
}
