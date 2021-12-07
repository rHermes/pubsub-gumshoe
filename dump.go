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
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

type MessageJson struct {
	// ID identifies this message. This ID is assigned by the server and is
	// populated for Messages obtained from a subscription.
	//
	// This field is read-only.
	ID string

	// Data is the actual data in the message.
	Data json.RawMessage

	// Attributes represents the key-value pairs the current message is
	// labelled with.
	Attributes map[string]string

	// PublishTime is the time at which the message was published. This is
	// populated by the server for Messages obtained from a subscription.
	//
	// This field is read-only.
	PublishTime time.Time

	// DeliveryAttempt is the number of times a message has been delivered.
	// This is part of the dead lettering feature that forwards messages that
	// fail to be processed (from nack/ack deadline timeout) to a dead letter topic.
	// If dead lettering is enabled, this will be set on all attempts, starting
	// with value 1. Otherwise, the value will be nil.
	// This field is read-only.
	DeliveryAttempt *int

	// OrderingKey identifies related messages for which publish order should
	// be respected. If empty string is used, message will be sent unordered.
	OrderingKey string
}

func DumpTopicCmd(c *cli.Context) error {
	client := getPubsubClient(c.Context)

	topicName := c.String("topic")
	outputFile := c.Path("out")
	rewindDur := c.Duration("rewind")
	// We have a deadline here, to estimate buffer output complition
	waitDeadline := c.Duration("wait")

	rawOputput := c.Bool("raw")

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

	tk := time.Now()

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

	jWriter := json.NewEncoder(outFile)

	var messages uint64
	lastMessageSeen := time.Now()
outer:
	for {
		select {
		case <-cc.Done():
			log.Printf("Context canceled, ending dump.")
			break outer

		case msg := <-outChan:
			messages += 1
			if messages%100 == 0 {
				log.Printf("We have %d messages, last id is: %s", messages, msg.ID)
			}

			lastMessageSeen = time.Now()

			if rawOputput {
				if err := jWriter.Encode(&msg); err != nil {
					return fmt.Errorf("writing to outputfile: %v", err)
				}
			} else {
				jmsg := MessageJson{
					ID:              msg.ID,
					Data:            msg.Data,
					Attributes:      msg.Attributes,
					PublishTime:     msg.PublishTime,
					DeliveryAttempt: msg.DeliveryAttempt,
					OrderingKey:     msg.OrderingKey,
				}
				if err := jWriter.Encode(&jmsg); err != nil {
					return fmt.Errorf("writing to outputfile: %v", err)
				}
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

	// dur := time.Since(tk)
	dur := lastMessageSeen.Sub(tk)
	secs := dur.Seconds()
	log.Printf("Rewind took %s and we saw %d messages, making up a rate of %.2f messages per second", dur.String(), messages, float64(messages)/secs)

	return nil
}
