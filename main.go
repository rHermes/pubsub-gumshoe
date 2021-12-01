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
	"google.golang.org/api/iterator"
)

// func RootCmd(c *cli.Context) error {
// 	return nil
// }

func ListTopicsCmd(c *cli.Context) error {
	client := getPubsubClient(c.Context)

	it := client.Topics(c.Context)
	for {
		t, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("listing topics: %v", err)
		}

		fmt.Printf("Topic: %s\n", t.ID())
	}
	return nil
}

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

func main() {
	app := &cli.App{
		Name:  "pubsub-gumshoe",
		Usage: "Investigate pubsubbery",
		// Action: RootCmd,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "project",
				Usage:    "The gcp project id",
				Required: true,
			},
		},
		Before: func(c *cli.Context) error {
			client, err := pubsub.NewClient(c.Context, c.String("project"))
			if err != nil {
				return fmt.Errorf("creating pubsub client: %v", err)
			}

			c.Context = setPubsubClient(c.Context, client)
			return nil
		},
		After: func(c *cli.Context) error {
			client := getPubsubClient(c.Context)
			client.Close()
			return nil
		},
		Commands: []*cli.Command{
			{
				Name:  "topics",
				Usage: "operations on topics",
				Subcommands: []*cli.Command{
					{
						Name:   "list",
						Usage:  "list topics",
						Action: ListTopicsCmd,
					},
					{
						Name:   "monitor",
						Usage:  "Monitor a topic",
						Action: MonitorTopicCmd,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "topic",
								Usage:    "The topic id to monitor",
								Required: true,
							},
						},
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
