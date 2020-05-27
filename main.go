package main

import (
	"crypto/rsa"
	"github.com/joho/godotenv"
	"github.com/ufcg-lsd/arrebol-pb-worker/utils"
	"github.com/ufcg-lsd/arrebol-pb-worker/worker"
	"log"
	"os"
)

const (
	ConfFilePathKey = "CONF_FILE_PATH"
	ServerEndpointKey = "SERVER_ENDPOINT"
)

func generateKeys(workerId string) {
	log.Println("Starting to gen rsa key pair with workerid: " + workerId)
	utils.GenAccessKeys(workerId)
}

//This functions sends the worker's public key to the server
func sendKey(serverEndPoint string, workerId string) {
	log.Println("Sending pub key to the server. ServerEndpoint: " + serverEndPoint)
	url := serverEndPoint + "/workers/publicKey"
	requestBody := &map[string]*rsa.PublicKey{"key": utils.GetPublicKey(workerId)}
	httpResp := utils.Post(requestBody, url)

	if httpResp.StatusCode != 201 {
		log.Fatal("Unable to send public key to the server")
	}
}

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Println("No .env file found")
	}

	startWorker()
}

func startWorker() {
	// This is the default work behavior implementation.
	// Its core stands for executing one task at a time.
	log.Println("Starting reading configuration process")
	file, err := os.Open(os.Getenv(ConfFilePathKey))

	if err != nil {
		log.Fatal("Error on opening configuration file", err.Error())
	}

	defer file.Close()

	workerInstance := worker.ParseWorkerConfiguration(file)

	serverEndpoint := os.Getenv(ServerEndpointKey)

	//before join the server, the worker must send its public key
	generateKeys(workerInstance.Id)
	sendKey(serverEndpoint, workerInstance.Id)

	for {
		isTokenValid := utils.CheckToken(workerInstance.Token)
		if !isTokenValid {
			workerInstance.Join(serverEndpoint)
		}

		task, err := workerInstance.GetTask(serverEndpoint)

		if err != nil {
			//it will force the worker to Join again, if the error has occurred because of
			//authentication issues. This is a work arround while the system doesn't have
			//its own Error module that will allow it to identify the error type.
			log.Println(err.Error())
			continue
		}

		log.Println(task.Id)
	}
}