/*
Copyright 2021 Teodor Spæren
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

func DumpTopicCmd(c *cli.Context) error {
	client := getPubsubClient(c.Context)

	topicName := c.String("topic")
	outputFile := c.Path("out")
	rewindDur := c.Duration("rewind")
	// We have a deadline here, to estimate buffer output complition
	waitDeadline := c.Duration("wait")

	// Attempt to open the file for writing
	outFile, err := os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("opening output file: %v", err)
	}
	defer outFile.Close()

	top := client.Topic(topicName)

	ui := uuid.New()
	id := fmt.Sprintf("pubsub-gumshoe-%s", ui.String())

	cc, cancel := signal.NotifyContext(c.Context, os.Interrupt)
	defer cancel()

	sub, err := client.CreateSubscription(cc, id, pubsub.SubscriptionConfig{
		ExpirationPolicy: time.Hour * 24,
		Topic:            top,
		Labels: map[string]string{
			"created_by": "pubsub-gumshoe",
		},
		Detached: false,
	})
	if err != nil {
		return fmt.Errorf("creating subscription: %v", err)
	}
	defer func() {
		log.Printf("Attempting to delete temp subscription: %s", sub.ID())
		if err := sub.Delete(c.Context); err != nil {
			log.Printf("deleting temp subscription: %v", err)
		} else {
			log.Printf("deleted temp subscription")
		}
	}()

	// We seek back in time
	if err := sub.SeekToTime(cc, time.Now().Add(-rewindDur)); err != nil {
		return fmt.Errorf("rewiding subscription: %v", err)
	}

	outChan := make(chan *pubsub.Message, 10)

	cw, fcancel := context.WithCancel(cc)
	defer fcancel()

	go func(ctx context.Context) {
		log.Printf("Starting dump")
		err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			outChan <- msg
		})
		if err != nil {
			log.Printf("receiving from sub: %v", err)
		}
		// close(outChan)
	}(cw)

	tic := time.NewTimer(waitDeadline)
	defer tic.Stop()

outer:
	for {
		select {
		case <-cc.Done():
			log.Printf("Context canceled, ending dump.")
			break outer

		case msg := <-outChan:
			log.Printf("We got a message with id: %s", msg.ID)

			if _, err := outFile.Write(msg.Data); err != nil {
				return fmt.Errorf("writing to outputfile: %v", err)
			}
			if _, err := fmt.Fprintln(outFile, ""); err != nil {
				return fmt.Errorf("writing to outputfile: %v", err)
			}

			msg.Ack()

			// Now we must kick the ticker, to make sure it's not fired
			tic.Reset(waitDeadline)
		case <-tic.C:
			log.Printf("Haven't seen any messages in %s, assuming dump to be done.", waitDeadline.String())

			fcancel()
			break outer
		}
	}

	return nil
}
