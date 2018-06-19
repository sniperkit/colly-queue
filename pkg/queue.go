package queue

import (
	"net/url"
	"sync"
	"sync/atomic"

	storage "github.com/sniperkit/colly-storage/pkg"
	colly "github.com/sniperkit/colly/pkg"
)

const stop = true

// Queue is a request queue which uses a Collector to consume
// requests in multiple threads
type Queue struct {
	// Threads defines the number of consumer threads
	Threads           int
	storage           storage.Storage
	activeThreadCount int32
	threadChans       []chan bool
	lock              *sync.Mutex
}

// New creates a new queue with a Storage specified in argument
// A standard InMemoryQueueStorage is used if Storage argument is nil.
func New(threads int, s storage.Storage) (*Queue, error) {
	if s == nil {
		s = &InMemoryQueueStorage{MaxSize: 100000}
	}
	if err := s.Init(); err != nil {
		return nil, err
	}
	return &Queue{
		Threads:     threads,
		storage:     s,
		lock:        &sync.Mutex{},
		threadChans: make([]chan bool, 0, threads),
	}, nil
}

// IsEmpty returns true if the queue is empty
func (q *Queue) IsEmpty() bool {
	s, _ := q.Size()
	return s == 0
}

// AddURL adds a new URL to the queue
func (q *Queue) AddURL(URL string) error {
	u, err := url.Parse(URL)
	if err != nil {
		return err
	}
	r := &colly.Request{
		URL:    u,
		Method: "GET",
	}
	d, err := r.Marshal()
	if err != nil {
		return err
	}
	return q.storage.Set(u.String(), d)
}

// AddRequest adds a new Request to the queue
func (q *Queue) AddRequest(r *colly.Request) error {
	d, err := r.Marshal()
	if err != nil {
		return err
	}

	if err := q.storage.Set(string(d), d); err != nil {
		return err
	}
	q.lock.Lock()
	for _, c := range q.threadChans {
		c <- !stop
	}
	q.threadChans = make([]chan bool, 0, q.Threads)
	q.lock.Unlock()
	return nil
}

// Size returns the size of the queue
func (q *Queue) Size() (int, error) {
	res, err := q.storage.Action("size", nil)
	return res["size"].(int), err
}

// Run starts consumer threads and calls the Collector
// to perform requests. Run blocks while the queue has active requests
func (q *Queue) Run(c *colly.Collector) error {
	wg := &sync.WaitGroup{}
	for i := 0; i < q.Threads; i++ {
		wg.Add(1)
		go func(c *colly.Collector, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				if q.IsEmpty() {
					if q.activeThreadCount == 0 {
						break
					}
					ch := make(chan bool)
					q.lock.Lock()
					q.threadChans = append(q.threadChans, ch)
					q.lock.Unlock()
					action := <-ch
					if action == stop && q.IsEmpty() {
						break
					}
				}
				q.lock.Lock()
				atomic.AddInt32(&q.activeThreadCount, 1)
				q.lock.Unlock()
				rb, ok := q.storage.Get("test")
				if !ok || rb == nil {
					q.finish()
					continue
				}
				r, err := c.UnmarshalRequest(rb)
				if err != nil || r == nil {
					q.finish()
					continue
				}
				r.Do()
				q.finish()
			}
		}(c, wg)
	}
	wg.Wait()
	return nil
}

func (q *Queue) finish() {
	q.lock.Lock()
	q.activeThreadCount--
	for _, c := range q.threadChans {
		c <- stop
	}
	q.threadChans = make([]chan bool, 0, q.Threads)
	q.lock.Unlock()
}
