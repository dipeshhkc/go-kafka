package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/joho/godotenv"
)

func main() {

	// Uncomment this on development. Load your .env file on main.go file
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	topic := os.Getenv("TOPIC")
	worker, err := connectConsumer([]string{os.Getenv("BROKER_URL")})
	if err != nil {
		panic(err)
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started ")
	//set up a channel (sigchan) for receiving operating system signals.
	sigchan := make(chan os.Signal, 1)
	// The signal.Notify function is used to register the channel (sigchan) to receive specific signals.
	// In this case, it registers the channel to receive the SIGINT (interrupt signal) and SIGTERM (termination signal) signals from the OS.
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})
	// beginning of a goroutine (concurrently executing function)
	go func() {
		//an infinite loop
		for {
			//  The select statement is used in Go to choose between multiple channel operations.
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	// wait until signchan gets either of user interruption or program termination [Case 3]
	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}

}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
