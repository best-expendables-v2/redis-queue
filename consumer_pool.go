package redisqueue

import "github.com/best-expendables-v2/rmq"

type consumerPool struct {
	concurrency  int
	deliveryChan chan rmq.Delivery
}

func MakeConsumerPool(
	consumer rmq.Consumer,
	concurrency int,
) rmq.Consumer {
	pool := &consumerPool{
		concurrency:  concurrency,
		deliveryChan: make(chan rmq.Delivery, concurrency),
	}
	pool.run(consumer)
	return pool
}

func (c *consumerPool) run(consumer rmq.Consumer) {
	for i := 0; i < c.concurrency; i++ {
		go func() {
			for delivery := range c.deliveryChan {
				consumer.Consume(delivery)
			}
		}()
	}
}

func (c *consumerPool) Consume(delivery rmq.Delivery) {
	c.deliveryChan <- delivery
}
