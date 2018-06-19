package queue

import (
	"errors"
	"sync"
)

/*
// Storage is the interface of the queue's storage backend
type Storage interface {
	// Init initializes the storage
	Init() error
	// AddRequest adds a serialized request to the queue
	AddRequest([]byte) error
	// GetRequest pops the next request from the queue
	// or returns error if the queue is empty
	GetRequest() ([]byte, error)
	// QueueSize returns with the size of the queue
	QueueSize() (int, error)
}
*/

// InMemoryQueueStorage is the default implementation of the Storage interface.
// InMemoryQueueStorage holds the request queue in memory.
type InMemoryQueueStorage struct {
	// MaxSize defines the capacity of the queue.
	// New requests are discarded if the queue size reaches MaxSize
	MaxSize int
	lock    *sync.RWMutex
	size    int
	first   *inMemoryQueueItem
	last    *inMemoryQueueItem
}

type inMemoryQueueItem struct {
	Request []byte
	Next    *inMemoryQueueItem
}

// Init implements Storage.Init() function
func (q *InMemoryQueueStorage) Init() error {
	q.lock = &sync.RWMutex{}
	return nil
}

func (q *InMemoryQueueStorage) Get(key string) (resp []byte, ok bool) {
	return []byte{}, false
}

func (q *InMemoryQueueStorage) Set(key string, resp []byte) error {
	return errors.New("Set() method is not implemented yet")
}

func (q *InMemoryQueueStorage) Delete(key string) error {
	return errors.New("Delete() method is not implemented yet")
}

// Debug
func (q *InMemoryQueueStorage) Debug(action string) error {
	return errors.New("Debug() method is not implemented yet")
}

func (q *InMemoryQueueStorage) Action(name string, args ...interface{}) (map[string]interface{}, error) {
	return nil, errors.New("Action() method is not implemented yet")
}

// Ping check if the storage is available...
func (q *InMemoryQueueStorage) Ping() error {
	return errors.New("Ping() method is not implemented yet")
}

// Clear truncate all key/values stored...
func (q *InMemoryQueueStorage) Clear() error {
	return errors.New("Debug is not implemented yet")
}

// Close deletes the storage
func (q *InMemoryQueueStorage) Close() error {
	return nil
}

/*
// AddRequest implements Storage.AddRequest() function
func (q *InMemoryQueueStorage) AddRequest(r []byte) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	// Discard URLs if size limit exceeded
	if q.MaxSize > 0 && q.size >= q.MaxSize {
		return nil
	}
	i := &inMemoryQueueItem{Request: r}
	if q.first == nil {
		q.first = i
	} else {
		q.last.Next = i
	}
	q.last = i
	q.size++
	return nil
}

// GetRequest implements Storage.GetRequest() function
func (q *InMemoryQueueStorage) GetRequest() ([]byte, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.size == 0 {
		return nil, nil
	}
	r := q.first.Request
	q.first = q.first.Next
	q.size--
	return r, nil
}

// QueueSize implements Storage.QueueSize() function
func (q *InMemoryQueueStorage) QueueSize() (int, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.size, nil
}
*/
