package main

import (
	"bytes"
	"crypto/rsa"
	"encoding/json"
	"github.com/ufcg-lsd/arrebol-pb-worker/worker"
	"log"
	"net/http"
	"os"
)

func setup(serverEndPoint string, workerId string) {
	// The setup routine is responsible for generates the rsa key pairs
	// of the worker and to send the public part to the server.
	log.Println("Starting to gen rsa key pair with workerid: " + workerId)

	worker.Gen(workerId)

	log.Println("Sending pub key to the server")
	url := serverEndPoint + "/workers/publicKey"
	requestBody, err := json.Marshal(&map[string]*rsa.PublicKey{"key": worker.GetPublicKey(workerId)})

	log.Println("url: " + url)
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(requestBody))
	if err != nil {
		// handle error
		log.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		// handle error
		log.Fatal(err)
		panic(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		log.Fatal("Unable to send public key to the server")
		panic(err)
	}
}

func isTokenValid(token string) bool {
	return true
}

func main() {
	// this main function start the worker following the chosen implementation
	// passed by arg in the cli. The defaultWorker is started if no arg has been received.
	switch len(os.Args) {
	case 2:
		workerImpl := os.Args[1]
		println(workerImpl)
	default:
		defaultWorker()
	}
}

func defaultWorker() {
	// This is the default work behavior implementation.
	// Its core stands for executing one task at a time.
	workerInstance := worker.LoadWorker()
	setup(workerInstance.ServerEndPoint, workerInstance.Id)
	workerInstance.Subscribe()
	for {
		if !isTokenValid(workerInstance.Token) {
			workerInstance.Subscribe()
		}
		//task := worker.getTask()
		// worker.execTask(task)
	}
}
