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
