package future

import (
	"fmt"
	"sync"
)

type Future interface {
	Value() interface{}
	SetValue(value interface{})
}

func NewFuture() Future {
	return &future{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

type future struct {
	value interface{}
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

func (f *future) SetValue(value interface{}) {
	f.cond.L.Lock()
	if !f.set {
		f.value = value
		f.set = true
		f.cond.Broadcast()
	}
	f.cond.L.Unlock()
}

func (f *future) String() string {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	if !f.set {
		return "[future: pending]"
	} else {
		return fmt.Sprintf("[future: value [%T] = %+v]", f.value, f.value)
	}
}
