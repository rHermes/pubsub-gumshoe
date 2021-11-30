package main

import (
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
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
					},
					// {
					// 	Name:  "dump",
					// 	Usage: "dump a specific topic.",
					// },
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
