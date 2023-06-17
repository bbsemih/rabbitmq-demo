package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	//Advanced Message Queuing Protocol (AMQP) enables message direction, queuing, routing (including point-to-point and publish-and-subscribe), reliability and security
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	//Channel opens a unique, concurrent server channel
	// to process the bulk of AMQP messages.
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", //name (queue name)
		false,   //durable (queue will survive a server restart or not)
		false,   //delete when unused (queue is deleted when last consumer unsubscribes or not)
		false,   //exclusive (exclusive queue can only be accessed by the current connection and will be deleted when the connection is closed)
		false,   //no-wait (whether the queue should be declared as non-blocking. if true, the server will not respond to the declaration)
		nil,     //arguments
	)

	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "Hello World!"
	//exchange connected to queues using bindings (binding key is like a routing key)
	err = ch.PublishWithContext( //publishes a message onto the queue
		ctx,    //context
		"",     //exchange (exchang is used to route the message to the desired queue if the queue name is not specified)
		q.Name, //routing key
		false,  //mandatory
		false,  //immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf(fmt.Sprintf("%s: %s", msg, err))
	}
}
