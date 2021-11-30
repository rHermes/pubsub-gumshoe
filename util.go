package main

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type ctxKey string

var ctxKeyPubsubClient = ctxKey("pubsub")

func setPubsubClient(ctx context.Context, client *pubsub.Client) context.Context {
	return context.WithValue(ctx, ctxKeyPubsubClient, client)
}

func getPubsubClient(ctx context.Context) *pubsub.Client {
	client, ok := ctx.Value(ctxKeyPubsubClient).(*pubsub.Client)
	if !ok {
		panic("cannot get client")
	}
	return client
}
