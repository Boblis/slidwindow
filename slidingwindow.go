package slidingwindow

import (
	"sync"
	"time"
)

type Window interface {
	Start() time.Time

	Count() int64

	AddCount(n int64)

	Reset(s time.Time, c int64)

	Sync(now time.Time)
}

type StopFunc func()

type NewWindow func() (Window, StopFunc)

type Limiter struct {
	size  time.Duration
	limit int64

	mu sync.Mutex

	curr Window
	prev Window
}

func NewLimiter(size time.Duration, limit int64, newWindow NewWindow) (*Limiter, StopFunc) {
	currWin, currStop := newWindow()

	prevWin, _ := NewLocalWindow()

	lim := &Limiter{
		size:  size,
		limit: limit,
		curr:  currWin,
		prev:  prevWin,
	}

	return lim, currStop
}

func (lim *Limiter) Size() time.Duration {
	return lim.size
}

func (lim *Limiter) Limit() int64 {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.limit
}

func (lim *Limiter) SetLimit(newLimit int64) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	lim.limit = newLimit
}

func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

func (lim *Limiter) AllowN(now time.Time, n int64) bool {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	lim.advance(now)

	elapsed := now.Sub(lim.curr.Start())
	weight := float64(lim.size-elapsed) / float64(lim.size)
	count := int64(weight*float64(lim.prev.Count())) + lim.curr.Count()

	defer lim.curr.Sync(now)

	if count+n > lim.limit {
		return false
	}

	lim.curr.AddCount(n)
	return true
}

func (lim *Limiter) advance(now time.Time) {
	newCurrStart := now.Truncate(lim.size)

	diffSize := newCurrStart.Sub(lim.curr.Start()) / lim.size
	if diffSize >= 1 {

		newPrevCount := int64(0)
		if diffSize == 1 {
			newPrevCount = lim.curr.Count()
		}
		lim.prev.Reset(newCurrStart.Add(-lim.size), newPrevCount)

		lim.curr.Reset(newCurrStart, 0)
	}
}
