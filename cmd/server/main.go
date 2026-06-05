package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitURL = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	log.Println("connection to broker was successful")

	channel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer channel.Close()

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		event := words[0]
		switch event {
		case "pause":
			fmt.Println("sending a pause message")
			err := pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Println(err)
			}
		case "resume":
			fmt.Println("sending a resume message")
			err := pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Println(err)
			}
		case "quit":
			fmt.Println("exiting!")
			return
		default:
			fmt.Printf("unknown event: %s\n", event)
		}
	}
}
