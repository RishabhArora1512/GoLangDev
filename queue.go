import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type redisQueue struct {
	name           string
	connectionName string
	consumersKey   string // key to set of consumers using this connection
	producerKey string // key to set of producers using this connection
	queuesKey      string
	errChan        chan<- error
	prefetchLimit  int64 // max number of prefetched deliveries number of unacked can go up to prefetchLimit + numConsumers
	pollDuration   time.Duration
	consumingStopped chan struct{} 
	producingStopped chan struct{} 
	pollDuration   time.Duration
	lock             sync.Mutex    // protects the fields below related to starting and stopping this queue
	stopWg           sync.WaitGroup
	deliveryChan     chan Delivery // nil for publish channels, not nil for consuming channels
}
func newQueue(
	name string,
	connectionName string,
	queuesKey string,
	redisClient RedisClient,
	errChan chan<- error,
) *redisQueue {
	queue := &redisQueue{
		name:             name,
		connectionName:   connectionName,
		consumersKey:     consumersKey,
		queuesKey:        queuesKey,
		producerKey: producerKey,
		consumingStopped: consumingStopped,
		producingStopped: producingStopped,
		errChan:          errChan,
		
		redisClient:      redisClient
	}
	return queue
}






















