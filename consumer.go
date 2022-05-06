func (queue *redisQueue) AddConsumer(tag string, consumer Consumer) (name string, err error) {
	name, err = queue.addConsumer(tag)
	if err != nil {
		return "", err
	}
	go queue.consumerConsume(consumer)
	return name, nil
}

func (queue *redisQueue) consumerConsume(consumer Consumer) {
	defer func() {
		queue.stopWg.Done()
	}()
	for {
		select {
		case <-queue.consumingStopped:
			return
		default:
		}

		select {
		case <-queue.consumingStopped:
			return

		case delivery, ok := <-queue.deliveryChan:
			if !ok { 
				return
			}

			consumer.Consume(delivery)
		}
	}
}


func (queue *redisQueue) StartConsuming(prefetchLimit int64, pollDuration time.Duration) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	// If deliveryChan is set, then we are already consuming
	if queue.deliveryChan != nil {
		return ErrorAlreadyConsuming
	}
	select {
	case <-queue.consumingStopped:
		return ErrorConsumingStopped
	default:
	}

	if _, err := queue.redisClient.SAdd(queue.queuesKey, queue.name); err != nil {
		return err
	}

	queue.prefetchLimit = prefetchLimit
	queue.pollDuration = pollDuration
	queue.deliveryChan = make(chan Delivery, prefetchLimit)
	// log.Printf("rmq queue started consuming %s %d %s", queue, prefetchLimit, pollDuration)
	go queue.consume()
	return nil
}

func (queue *redisQueue) consume() {
	errorCount := 0 // number of consecutive batch errors

	for {
		switch err := err {
		case nil: // success
			errorCount = 0

		case ErrorConsumingStopped:
			close(queue.deliveryChan)
			return

		default: // redis error
			errorCount++
			select { // try to add error to channel, but don't block
			case queue.errChan <- &ConsumeError{RedisErr: err, Count: errorCount}:
			default:
			}
		}
		time.Sleep(jitteredDuration(queue.pollDuration))
	}
}

func jitteredDuration(duration time.Duration) time.Duration {
	factor := 0.9 + rand.Float64()*0.2 // a jitter factor between 0.9 and 1.1 (+-10%)
	return time.Duration(float64(duration) * factor)
}