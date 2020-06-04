package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ufcg-lsd/arrebol-pb-worker/utils"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
	"github.com/dgrijalva/jwt-go"
)

const (
	PUBLIC_KEY = "PUBLIC-KEY"
)

//It represents each one of the worker's instances that will run on the worker node.
//The informations kept in this struct are important to the
//communication process with the server. While the Vcpu and Ram allow the server
//to choose better which tasks to dispatch to the worker instance, the Token, the Id
//and the QueueId are indispensable to establish the communication.
//Note: the Token is only SET when the worker joins the server.
//The QueueId can be SET during a join or in the worker's conf file.
//The others are set in the conf file.
type Worker struct {
	//The Vcpu available to the worker instance
	Vcpu float32
	//The Ram available to the worker instance
	Ram float32
	//The Token that the server has been assigned to the worker
	//so it is able to authenticate in next requests
	Token string
	//The worker instance id
	Id string
	//The queue from which the worker must ask for tasks
	QueueId string
}

const (
	WorkerNodeAddressKey = "WORKER_NODE_ADDRESS"
)
type TaskState uint8

const (
	TaskPending TaskState = iota
	TaskRunning
	TaskFinished
	TaskFailed
)

var (
	//for test purpose
	ParseToken func(tokenStr string) (map[string]interface{}, error) = parseToken
)

//This struct represents a task, the executable piece of the system.
type Task struct {
	// Sequence of unix command to be execute by the worker
	Commands       []string
	// Period (in seconds) between report status from the worker to the server
	ReportInterval int64
	State          TaskState
	// Indication of task completion progress, ranging from 0 to 100
	Progress       int
	// Docker image used to execute the task (e.g library/ubuntu:tag).
	DockerImage string
	Id string
}

func (ts TaskState) String() string {
	return [...]string{"TaskPending ", "TaskRunning", "TaskFinished", "TaskFailed"}[ts]
}

func (w *Worker) Join(serverEndpoint string) {
	headers := http.Header{}
	publicKey := utils.GetPublicKey(w.Id)
	parsedKey, err := json.Marshal(publicKey)

	if err != nil {
		log.Fatal("error on marshalling public key")
	}

	headers.Set(PUBLIC_KEY, string(parsedKey))
	httpResponse, err := utils.Post(w.Id, w, headers, serverEndpoint + "/workers")

	if err != nil {
		log.Fatal("Error on joining the server: " + err.Error())
	}

	HandleJoinResponse(httpResponse, w)
}

func HandleJoinResponse(response *utils.HttpResponse, w *Worker) {
	if response.StatusCode != 201 {
		log.Fatal("The work could not be subscribed")
	}

	var parsedBody map[string]string
	err := json.Unmarshal(response.Body, &parsedBody)

	if err != nil {
		log.Fatal("Unable to parse the response body")
	}

	token, ok := parsedBody["arrebol-worker-token"]

	if !ok {
		log.Fatal("The token is not in the response body")
	}

	parsedToken, err := ParseToken(token)

	if err != nil {
		log.Fatal(err)
	}

	queueId, ok := parsedToken["QueueId"]

	if !ok {
		log.Fatal("The queue_id is not in the response body")
	}

	w.Token = token
	w.QueueId = fmt.Sprintf("%v", queueId)
}

func (w *Worker) GetTask(serverEndPoint string) (*Task, error) {
	log.Println("Starting GetTask routine")

	if w.QueueId == "" {
		return nil, errors.New("The QueueId must be set before getting a task")
	}

	url := serverEndPoint + "/workers/" + w.Id + "/queues/" + w.QueueId + "/tasks"

	headers := http.Header{}
	headers.Set("arrebol-worker-token", w.Token)

	httpResp, err := utils.Get(w.Id, url, headers)

	if err != nil {
		return nil, errors.New("Error on GET request: " + err.Error())
	}

	respBody := httpResp.Body

	var task Task
	err = json.Unmarshal(respBody, &task)

	if err != nil {
		return nil, errors.New("Error on unmarshalling the task: " + err.Error())
	}

	return &task, nil
}

func ParseWorkerConfiguration(reader io.Reader) Worker {
	decoder := json.NewDecoder(reader)
	configuration := Worker{}
	err := decoder.Decode(&configuration)
	if err != nil {
		log.Println("Error on decoding configuration file", err.Error())
	}

	return configuration
}

func (w *Worker) ExecTask(task *Task, serverEndPoint string) {
	address := os.Getenv(WorkerNodeAddressKey)
	client := utils.NewDockerClient(address)
	taskExecutor := &TaskExecutor{Cli: *client}
	// This channel is used to warn the report routine about task completion
	endingChannel := make(chan interface{}, 1)
	// This channel is used to keep the ExecTask alive
	// until the last report is done
	waitChannel := make(chan interface{}, 1)
	// This channel is used to ping the report routine
	// when the execution container is ready.
	spawnWarner := make(chan interface{}, 1)
	reportChannels := []chan interface{}{endingChannel, waitChannel, spawnWarner}

	// The report routine must run concurrently with the execute one
	go w.reportTask(task, taskExecutor, reportChannels, serverEndPoint)

	err := taskExecutor.Execute(task, spawnWarner)

	if err != nil {
		log.Fatal(err.Error())
	}

	endingChannel <- "done"
	<-waitChannel
}

func (w *Worker) reportTask(task *Task, executor *TaskExecutor, channels []chan interface{}, serverEndPoint string) {
	// The report can only begin when the container
	// is ready.
	<-channels[2]
	startTime := time.Now().Unix()
	for {
		select {
		//in case the task is done
		case <-channels[0]:
			task.Progress = 100
			reportReq(w, task, serverEndPoint)
			// Tell the ExecTask it can finish
			channels[1] <- "done"
			return
		default:
			executedCmdsLen, err := executor.Track()

			if err != nil {
				log.Println(err)
			}

			task.Progress = executedCmdsLen * 100 / len(task.Commands)

			log.Println("progess: " + strconv.Itoa(task.Progress))

			currentTime := time.Now().Unix()
			if currentTime-startTime < task.ReportInterval {
				time.Sleep(1 * time.Second)
				continue
			}

			reportReq(w, task, serverEndPoint)

			startTime = currentTime
		}
	}
}

func reportReq(w *Worker, task *Task, serverEndPoint string) {
	url := serverEndPoint + "/workers/" + w.Id + "/queues/" + w.QueueId + "/tasks"

	header := http.Header{}
	header.Set("arrebol-worker-token", w.Token)

	resp, err := utils.Put(w.Id, task, header, url)

	if err != nil || resp.StatusCode != 200 {
		log.Println("Error on reporting task: " + err.Error())
	}
}

func parseToken(tokenStr string) (map[string]interface{}, error){
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		return utils.GetPublicKey("server"), nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		return claims, nil
	} else {
		return nil, errors.New("Error on parsing token")
	}
}
