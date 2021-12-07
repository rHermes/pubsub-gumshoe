package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

// RewindTestCmd tests how long it takes to catch up to some topics is supposed to do.
func RewindTestCmd(c *cli.Context) error {
	client := getPubsubClient(c.Context)
	rewindDur := c.Duration("rewind")
	// We have a deadline here, to estimate buffer output complition
	waitDeadline := c.Duration("wait")
	topicNames := c.StringSlice("topics")

	rewindDur = rewindDur
	waitDeadline = waitDeadline

	cc, cancel := signal.NotifyContext(c.Context, os.Interrupt)
	defer cancel()

	var mu sync.Mutex
	var wg sync.WaitGroup
	topics := make(map[string]*pubsub.Subscription, len(topicNames))
	var perr error
	for _, topic := range topicNames {
		wg.Add(1)
		// Creating subscriptions.
		go func(ctx context.Context, topicName string) {
			defer wg.Done()

			log.Printf("Starting creation of %s", topicName)

			top := client.Topic(topicName)

			ui := uuid.New()
			id := fmt.Sprintf("pubsub-gumshoe-%s", ui.String())

			sub, err := client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{
				ExpirationPolicy: time.Hour * 24,
				Topic:            top,
				Labels: map[string]string{
					"created_by": "pubsub-gumshoe",
				},
				Detached: false,
			})
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				perr = fmt.Errorf("creating subscription: %v", err)
				return
			}

			log.Printf("rewinding %s", topicName)

			// We seek back in time
			if err := sub.SeekToTime(ctx, time.Now().Add(-rewindDur)); err != nil {
				mu.Lock()
				defer mu.Unlock()
				perr = fmt.Errorf("rewiding subscription: %v", err)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			topics[topicName] = sub

			log.Printf("Finished creating %s", sub.ID())
		}(cc, topic)
	}
	wg.Wait()

	defer func(ctx context.Context) {
		var wg sync.WaitGroup
		for _, sub := range topics {
			wg.Add(1)
			go func(sub *pubsub.Subscription) {
				defer wg.Done()

				log.Printf("Attempting to delete temp subscription: %s", sub.ID())
				if err := sub.Delete(c.Context); err != nil {
					log.Printf("deleting temp subscription: %v", err)
				}
			}(sub)
		}
		wg.Wait()
	}(c.Context)

	// Start the ingestion
	chans := make([]<-chan *pubsub.Message, 0, len(topics))
	for topic, sub := range topics {
		chans = append(chans, drainTopic(cc, waitDeadline, topic, sub))
	}

	onechan := merge(chans...)
	nmessages := 0
	ts := time.Now()
	for msg := range onechan {
		nmessages += 1

		// if nmessages%100 == 0 {
		// 	log.Printf("We currently have %d messages.", nmessages)
		// }

		msg.Ack()
	}
	dur := time.Since(ts)

	log.Printf("Rewind took %s", dur.String())
	return nil
}

func drainTopic(ctx context.Context, timeout time.Duration, topic string, sub *pubsub.Subscription) <-chan *pubsub.Message {
	wctx, cancel := context.WithCancel(ctx)

	ch := make(chan *pubsub.Message)
	go func() {
		err := sub.Receive(wctx, func(pctx context.Context, msg *pubsub.Message) {
			ch <- msg
		})
		if err != nil {
			log.Printf("ERR: [%s]: [%s]", topic, err.Error())
		}
		log.Printf("We excited: %s", topic)
		close(ch)
	}()

	che := make(chan *pubsub.Message)
	go func() {
		tic := time.NewTimer(timeout)
		defer tic.Stop()
		start := time.Now()
		last := time.Now()

		messages := 0
	outer:
		for {
			select {
			case msg := <-ch:
				if msg.PublishTime.Before(time.Now().Add(-5 * time.Minute)) {
					last = time.Now()
					tic.Reset(timeout)
				}
				messages += 1
				che <- msg
			case <-tic.C:
				log.Printf("We are caught up on topic %s. It took %s and we saw %d messages", topic, last.Sub(start).String(), messages)
				break outer

			}
		}

		cancel()

		for msg := range ch {
			che <- msg
		}

		close(che)
	}()

	return che
}

func merge(cs ...<-chan *pubsub.Message) <-chan *pubsub.Message {
	var wg sync.WaitGroup
	out := make(chan *pubsub.Message, 10)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan *pubsub.Message) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
