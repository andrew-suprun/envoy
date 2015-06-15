package future

import (
	"sync"
)

type Future interface {
	Value() interface{}
	Error() error
	SetValue(value interface{})
	SetError(err error)
}

func NewFuture() Future {
	return &future{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

type future struct {
	value interface{}
	err   error
	cond  *sync.Cond
	set   bool
}

func (f *future) Value() interface{} {
	for {
		f.cond.L.Lock()
		if f.set {
			f.cond.L.Unlock()
			return f.value
		}
		f.cond.Wait()
		f.cond.L.Unlock()
	}
}

func (f *future) Error() error {
	for {
		f.cond.L.Lock()
		if f.set {
			f.cond.L.Unlock()
			return f.err
		}
		f.cond.Wait()
		f.cond.L.Unlock()
	}
}

func (f *future) SetValue(value interface{}) {
	f.cond.L.Lock()
	if !f.set {
		f.value = value
		f.set = true
	}
	f.cond.L.Unlock()
}

func (f *future) SetError(err error) {
	f.cond.L.Lock()
	if !f.set {
		f.err = err
		f.set = true
	}
	f.cond.L.Unlock()
}
