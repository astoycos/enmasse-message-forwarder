package main

import (
	"context"
	"flag"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/kelseyhightower/envconfig"
	"knative.dev/eventing-contrib/pkg/kncloudevents"
	"log"
	"os"
	"pack.ag/amqp"
	"strings"
)

var (
	eventSource string
	eventType   string
	sink        string
	messageChan chan *amqp.Message = make(chan *amqp.Message) //# of devices Connected to System
)

func init() {
	flag.StringVar(&eventSource, "eventSource", "", "the event-source (CloudEvents)")
	flag.StringVar(&eventType, "eventType", "github.com/astoycos/iotContainerSource", "the event-type (CloudEvents)")
	flag.StringVar(&sink, "sink", "", "the service url to send data to to")
}

//Struct to hold loaded environment variables from custom .YAML
type envConfig struct {
	//Sink URL where to send IOT cloudevents
	Sink string `envconfig:"K_SINK"`

	//Name of this pod.
	Name string `envconfig:"POD_NAME" required:"true"`

	//Namespace this pod exists in.
	Namespace string `envconfig:"POD_NAMESPACE" required:"true"`

	//Whether to run continuously or exit.
	OneShot bool `envconfig:"ONE_SHOT" default:"false"`

	//Enmasse Endpoint configs

	//Messaging endpoing URI
	MessageURI string `envconfig:"MESSAGE_URI" required:"true"`

	//Messaging Port
	MessagePort string `envconfig:"MESSAGE_PORT" required:"true"`

	//Message Type telemetry/event
	MessageType string `envconfig:"MESSAGE_TYPE" required:"true"`

	//Messaging Tenant
	MessageTenant string `envconfig:"MESSAGE_TENANT" required:"true"`

	//TLS Setting 0:No TLS 1: TLS INSECURE 2:TLS Secure
	TLSConfig int `envconfig:"TLS_CONFIG" default:"0"`

	//Path to TLS credentials
	TLSCert string `envconfig:"TLS_CERT" default:""`

	//hono client Username
	ClientUsername string `envconfig:"CLIENT_USERNAME" default:""`

	//hono client Password
	ClientPassword string `envconfig:"CLIENT_PASSWORD" default:"" `
}

func main() {

	//Load in Env variables

	flag.Parse()

	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Printf("[ERROR] Failed to process env var: %s", err)
		os.Exit(1)
	}

	if env.Sink != "" {
		sink = env.Sink
		log.Println("Sink set by ENV")
	} else {
		log.Println("Sink set by Yaml")
	}

	if eventSource == "" {
		eventSource = fmt.Sprintf("https://github.com/astoycos/iotContanerSource/#%s/%s", env.Namespace, env.Name)
	}

	//Start Consumer goroutine to listen for new IOT messages at Enmasse Endpoint
	go consume(env.MessageType, env.MessageURI, env.MessagePort, env.MessageTenant, env.ClientUsername, env.ClientPassword, env.TLSConfig, env.TLSCert, sink)

	/*
		//Infinite loop that waits for data from enmasse and then forwards it to kafka AMQP bridge
		for {
			log.Printf("Consuming %s data from enmasse endpoint: %s", env.MessageType, env.MessageURI)
			log.Printf("Consumer running, press Ctrl+C to stop...")

			//Channel to funnel messages from consuming goroutine
			msg := <-messageChan

			log.Printf("Device Data received")

			log.Printf("Making cloudevent")

			//Make Cloud Event
			//TODO: Make into own function
			event := cloudevents.NewEvent("1.0")
			event.SetType(eventType)
			event.SetSource(eventSource)
			event.SetDataContentType(msg.Properties.ContentType)
			for typemsg, value := range msg.Annotations {
				event.SetExtension(strings.ToLower(strings.ReplaceAll(typemsg.(string), "_", "")), strings.ToLower(strings.ReplaceAll(value.(string), "_", "")))
			}
			event.SetExtension("MessageType", msg.Properties.ContentType)
			event.SetDataContentType(msg.Properties.ContentType)

			if err := event.SetData(msg.GetData()); err != nil {
				log.Printf("Failed to set cloudevents data: %s", err.Error())
			}

			//Send cloud Event
			log.Printf("Sending cloudevent to %s", sink)
			if _, _, err := c.Send(context.Background(), event); err != nil {
				log.Printf("Failed to send cloudevent: %s", err.Error())
			}

			//ENV option to only receive once message
			if env.OneShot {
				return
			}

		}*/
}
