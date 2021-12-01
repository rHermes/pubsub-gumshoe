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

func MonitorTopicCmd(c *cli.Context) error {
	client := getPubsubClient(c.Context)
	topicName := c.String("topic")

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
	})
	if err != nil {
		return fmt.Errorf("creating subscription: %v", err)
	}
	defer func() {
		if err := sub.Delete(c.Context); err != nil {
			log.Printf("deleting temp topic: %v", err)
		} else {
			log.Printf("deleted temp topic")
		}
	}()

	log.Printf("Listening!")
	err = sub.Receive(cc, func(ctx context.Context, msg *pubsub.Message) {
		defer msg.Nack()

		fmt.Println(string(msg.Data))
		msg.Ack()
	})
	if err != nil {
		log.Printf("receiving from sub: %v", err)
	}

	return nil
}
