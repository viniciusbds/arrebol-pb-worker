package main

import (
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/ufcg-lsd/arrebol-pb-worker/utils"
	"github.com/ufcg-lsd/arrebol-pb-worker/worker"
)

const (
	ConfFilePathKey   = "CONF_FILE_PATH"
	ServerEndpointKey = "SERVER_ENDPOINT"
)

func generateKeys(workerId string) {
	log.Println("Starting to gen rsa key pair with workerid: " + workerId)
	utils.GenAccessKeys(workerId)
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

	//before join the server, the worker must generate the keys
	generateKeys(workerInstance.ID.String())

	workerInstance.Join(serverEndpoint)

	for {
		task, err := workerInstance.GetTask(serverEndpoint)
		time.Sleep(3 * time.Second)
		if err != nil {
			//it will force the worker to Join again, if the error has occurred because of
			//authentication issues. This is a work arround while the system doesn't have
			//its own Error module that will allow it to identify the error type.
			// workerInstance.Join(serverEndpoint)
			continue
		}

		workerInstance.ExecTask(task, serverEndpoint)
	}
}
