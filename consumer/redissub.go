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
