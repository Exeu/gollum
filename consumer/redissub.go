// Copyright 2015-2016 trivago GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumer

import (
	"sync"

	"github.com/trivago/gollum/core"
	"github.com/trivago/gollum/core/log"
	"github.com/trivago/gollum/shared"
	"gopkg.in/redis.v4"
)

type RedisSub struct {
	core.ConsumerBase
	address  string
	protocol string
	password string
	channel  string
	client   *redis.Client
	sequence *uint64
}

func init() {
	shared.TypeRegistry.Register(RedisSub{})
}

// Configure initializes this consumer with values from a plugin config.
func (cons *RedisSub) Configure(conf core.PluginConfig) error {
	err := cons.ConsumerBase.Configure(conf)
	if err != nil {
		return err
	}

	cons.password = conf.GetString("Password", "")
	cons.address, cons.protocol = shared.ParseAddress(conf.GetString("Address", ":6379"))
	cons.channel = conf.GetString("Channel", "defaultValue")
	cons.sequence = new(uint64)

	return nil
}

func (cons *RedisSub) receive(pubsub *redis.PubSub) error {
	defer cons.WorkerDone()
	for cons.IsActive() {
		msg, err := pubsub.ReceiveMessage()

		if err != nil {
			Log.Error.Println("Error receiving message: ", err)
			return err
		}

		cons.Enqueue([]byte(msg.Payload), *cons.sequence)
		*cons.sequence++
	}

	return nil
}

func (cons *RedisSub) Consume(workers *sync.WaitGroup) {
	cons.AddMainWorker(workers)
	cons.client = redis.NewClient(&redis.Options{
		Addr:     cons.address,
		Network:  cons.protocol,
		Password: cons.password,
	})

	if _, err := cons.client.Ping().Result(); err != nil {
		Log.Error.Print("Redis: ", err)
	}

	pubsub, err := cons.client.PSubscribe(cons.channel)
	if err != nil {
		Log.Error.Println("Error subscribing channel: ", err)
	}

	Log.Note.Println("Sucessfully subscribed channel: ", cons.channel)

	go cons.receive(pubsub)
	defer cons.client.Close()

	cons.ControlLoop()
}
