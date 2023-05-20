package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
)

// Message struct
type Message struct {
	Text string `form:"text" json:"text"`
}

func main() {

	// Uncomment this on development. Load your .env file on main.go file
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	app := fiber.New()
	api := app.Group("/api/v1")

	api.Post("/send-message", sendMessage)

	app.Listen(":8000")

}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushMessageToQueue(topic string, message []byte) error {

	brokersUrl := []string{os.Getenv("BROKER_URL")}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

// sendMessage handler
func sendMessage(c *fiber.Ctx) error {

	// Instantiate new Message struct
	cmt := new(Message)

	//  Parse body into comment struct
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	// convert body into bytes and send it to kafka
	cmtInBytes, err := json.Marshal(cmt)
	PushMessageToQueue(os.Getenv("TOPIC"), cmtInBytes)

	// Return Message in JSON format
	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Message pushed successfully",
		"comment": cmt,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error pushing",
		})
		return err
	}

	return err
}
