package consumer

import (
	"sync"

	"github.com/trivago/gollum/core"
	"github.com/trivago/gollum/core/log"
	"github.com/trivago/gollum/shared"
	"github.com/trivago/gollum/vendor/gopkg.in/redis.v4"
	"gopkg.in/redis.v4"
)

type RedisSub struct {
	core.ConsumerBase
	address  string
	protocol string
	password string
	database int
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

func (cons *RedisSub) receive(pubsub *redis.PubSub) {
	defer cons.WorkerDone()
	for cons.IsActive() {
		msg, err := pubsub.ReceiveMessage()

		if err != nil {
			Log.Error.Println("Error receiving message: ", err)
			return nil
		}

		cons.Enqueue([]byte(msg.Payload), *cons.sequence)
		*cons.sequence++
	}
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

	pubsub, err := cons.client.Subscribe(cons.channel)
	if err != nil {
		panic(err)
	}

	go cons.receive(pubsub)

	cons.ControlLoop()
}
