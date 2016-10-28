package consumer

import (
	"fmt"
	"sync"

	"github.com/trivago/gollum/core"
	"github.com/trivago/gollum/core/log"
	"github.com/trivago/gollum/shared"
	"gopkg.in/redis.v4"
)

type Redis struct {
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
	shared.TypeRegistry.Register(Redis{})
}

// Configure initializes this consumer with values from a plugin config.
func (cons *Redis) Configure(conf core.PluginConfig) error {
	err := cons.ConsumerBase.Configure(conf)
	if err != nil {
		return err
	}

	cons.password = conf.GetString("Password", "")
	cons.database = conf.GetInt("Database", 0)
	cons.address, cons.protocol = shared.ParseAddress(conf.GetString("Address", ":6379"))
	cons.channel = conf.GetString("Channel", "defaultValue")
	cons.sequence = new(uint64)

	return nil
}

func (cons *Redis) receive(pubsub *redis.PubSub) {
	for {
		msg, err := pubsub.ReceiveMessage()

		if err != nil {
			fmt.Println("error")
		}

		cons.Enqueue([]byte(msg.Payload), *cons.sequence)
		*cons.sequence++
	}
}

func (cons *Redis) Consume(workers *sync.WaitGroup) {
	cons.AddMainWorker(workers)
	cons.client = redis.NewClient(&redis.Options{
		Addr:     cons.address,
		Network:  cons.protocol,
		Password: cons.password,
		DB:       cons.database,
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
