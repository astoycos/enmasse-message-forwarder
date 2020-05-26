package main

import (
	"context"
	"fmt"
	"log"
	
	"time"
	"pack.ag/amqp"

	"crypto/tls"
	"crypto/x509"
)

//createTlsConfig creates the TLS configuration for conection to Enmasse Enpoint 
//inputs: 
//		tlsConfig int -> O: No TLS,1: TLS insecure, 2: TLS secure
//		tlsPath string -> Certificate information
//outputs:
//		*tls.Config -> tls.Config object pointer with information for AMQP connection  
func createTlsConfig(tlsConfig int,tlsCert string) *tls.Config {
	//Insecure TLS 
	if tlsConfig == 1 {
		return &tls.Config{
			InsecureSkipVerify:true,
		}
	//Secure TLS
	} else{
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(tlsCert))
		return &tls.Config{
			RootCAs: caCertPool,
		}
	}
}

//consume listens for new messages from iot devices on the Enmasse Endpoints
//inputs:
//		messageType string-> telemetry/event 
//		uri string-> Enmasse Messaging enpoint URI 
//		tenant string-> Enmasse Tenant name
//		clientUsername string-> Hono client Username 
//		clientPassword string-> Hono client Password 
// 		tlsConfig int -> O: No TLS,1: TLS insecure, 2: TLS secure
//		tlsPath string -> Absolute Path to cert file
//outputs: 
//		none 
func consume(messageType string, uri string,port string, tenant string,clientUsername string,clientPassword string, tlsConfig int, tlsCert string) error {

	opts := make([]amqp.ConnOption, 0)

	//Enable TLS if required
	if tlsConfig != 0 {
		opts = append(opts, amqp.ConnTLSConfig(createTlsConfig(tlsConfig,tlsCert)))
	}
	
	//Enable Client credentials if avaliable
	if(clientUsername != "" && clientPassword !=""){
		opts = append(opts, amqp.ConnSASLPlain(clientUsername, clientPassword))
	}
	uri = "amqps://" + uri + ":" + port
	client, err := amqp.Dial(uri, opts...)
	if err != nil {
		log.Fatal("AMQP dial failed to connect to Enmasse: ",err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			log.Fatal("Failed to close client:", err)
		}
	}()

	var ctx = context.Background()

	session, err := client.NewSession()
	if err != nil {
		return err
	}

	defer func() {
		if err := session.Close(ctx); err != nil {
			log.Fatal("Failed to close session:", err)
		}
	}()

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress(fmt.Sprintf("%s/%s", messageType, tenant)),
		amqp.LinkCredit(10),
	)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		if err := receiver.Close(ctx); err != nil {
			log.Fatal("Failed to close receiver: ", err)
		}
		cancel()
	}()

	// Inifinite for loop to continually receive messages from Enmasse Devices

	for {
		// Receive next message
		msg, err := receiver.Receive(ctx)
		if err != nil {
			return err
		}

		// Accept message
		if err := msg.Accept(); err != nil {
			return nil
		}

		//Make map with relevant device info
		var annotations  map[string][string]
		for typemsg, value := range msg.Annotations {
			annotations[typemsg.(string)] = value.(string)
		}

		// Create a sender
		sender, err := session.NewSender(
			amqp.LinkTargetAddress(sink + "/topics/" + annotations["device-id"]),
		)
		if err != nil {
			log.Fatal("Creating sender link:", err)
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)

		// Send message
		err = sender.Send(ctx, msg)
		if err != nil {
			log.Fatal("Sending message:", err)
		}

		sender.Close(ctx)
		cancel()

		
	}
}