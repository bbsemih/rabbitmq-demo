package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	//Advanced Message Queuing Protocol (AMQP) enables message direction, queuing, routing (including point-to-point and publish-and-subscribe), reliability and security
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ!")
	defer conn.Close()

	//Channel opens a unique, concurrent server channel
	// to process the bulk of AMQP messages.
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   //name
		"fanout", //type (fanout - broadcasts all the messages it receives to all the queues it knows)
		true,     //durable (queue will survive a broker restart)
		false,    //auto-deleted (queue is deleted when last consumer unsubscribes)
		false,    //internal (exchange cannot be directly published to by a client)
		false,    //no-wait (queue will assume to be declared on the server)
		nil,      //arguments
	)

	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(ctx,
		"logs", //exchange
		"",     //routing key is basically the queue name
		false,  //mandatory (if true, the server will return an unroutable message with a Return method)
		false,  //immediate (if true, the server will return an undeliverable message with a Return method)
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}

func bodyFrom(args []string) string {
	var s string
	if len(args) < 2 || os.Args[1] == "" {
		s = "hello....."
	} else {
		s = args[1]
	}
	return s
}
