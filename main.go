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
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/urfave/cli/v2"
	"google.golang.org/api/iterator"

	_ "net/http/pprof"
)

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

func main() {
	app := &cli.App{
		Name:  "pubsub-gumshoe",
		Usage: "Investigate pubsubbery",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "project",
				Usage:    "The gcp project id",
				Required: true,
			},
			&cli.BoolFlag{
				Name:  "pprof",
				Usage: "Enable pprof http interface",
				Value: false,
			},
		},
		Before: func(c *cli.Context) error {
			client, err := pubsub.NewClient(c.Context, c.String("project"))
			if err != nil {
				return fmt.Errorf("creating pubsub client: %v", err)
			}

			if c.Bool("pprof") {
				go func() {
					log.Println(http.ListenAndServe("localhost:6060", nil))
				}()
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
				Name:   "rewind-test",
				Usage:  "Test how long a rewind takes",
				Action: RewindTestCmd,
				Flags: []cli.Flag{
					&cli.DurationFlag{
						Name:     "rewind",
						Usage:    "How long to rewind the subscription",
						Required: true,
					},
					&cli.DurationFlag{
						Name:  "wait",
						Usage: "How long to wait for new messages, before exiting",
						Value: 10 * time.Second,
					},
					&cli.StringSliceFlag{
						Name:     "topics",
						Usage:    "Which topics to test rewind on",
						Required: true,
					},
				},
			},
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
					{
						Name:   "dump",
						Usage:  "dump messages from a topic into a file.",
						Action: DumpTopicCmd,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "topic",
								Usage:    "The topic id to monitor",
								Required: true,
							},
							&cli.PathFlag{
								Name:      "out",
								Usage:     "The file to use as output",
								TakesFile: true,
								Required:  true,
							},
							&cli.DurationFlag{
								Name:     "rewind",
								Usage:    "How long to rewind the subscription",
								Required: true,
							},
							&cli.DurationFlag{
								Name:  "wait",
								Usage: "How long to wait for new messages, before exiting",
								Value: 10 * time.Second,
							},
							&cli.BoolFlag{
								Name:  "raw",
								Usage: "dump the base64 encoded data, instead of json",
								Value: false,
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
