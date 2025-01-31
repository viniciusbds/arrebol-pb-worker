package worker

//This module implements all steps needed in the task execution, as follows:
//Init a container, which includes download the task's image; create and start the container;
//move the executor script to the work dir inside the container.
//Send the task commands as a file to the container
//Execute the task, which includes invoking the executor script passing the commands file as
//arg and keep tracking of the exit codes of each commands.
//Track the execution, by retrieving how many commands have already been executed.

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/ufcg-lsd/arrebol-pb-worker/utils"
)

const (
	TaskScriptExecutorFileName  = "task-script-executor.sh"
	RunTaskScriptCommandPattern = "/bin/bash %s -d -tsf=%s"
	DefaultWorkerDockerImage    = "ubuntu"
)

type TaskExecutor struct {
	Cli client.Client
	Cid string
}

func (e *TaskExecutor) Execute(task *Task, statesChanges chan<- TaskState) {
	image := task.DockerImage

	log.Println("Creating container with image: " + image)
	containerName := fmt.Sprintf("%v", task.ID) + "-" + strconv.Itoa(time.Now().Second())

	config := utils.ContainerConfig{
		Name:   containerName,
		Image:  image,
		Mounts: []mount.Mount{},
	}

	if err := e.init(config); err != nil {
		log.Println(err)
		statesChanges <- TaskFailed
		return
	}
	if err := e.send(task); err != nil {
		log.Println(err)
		statesChanges <- TaskFailed
		return
	}
	if err := e.run(fmt.Sprintf("%v", task.ID)); err != nil {
		log.Println(err)
		statesChanges <- TaskFailed
		return
	}
	utils.StopContainer(&e.Cli, e.Cid)
	utils.RemoveContainer(&e.Cli, e.Cid)
	statesChanges <- TaskFinished
}

func (e *TaskExecutor) init(config utils.ContainerConfig) error {
	exists, err := utils.CheckImage(&e.Cli, config.Image)
	if err != nil {
		return err
	}
	if !exists {
		if _, err = utils.Pull(&e.Cli, config.Image); err != nil {
			return err
		}
	}
	cid, err := utils.CreateContainer(&e.Cli, config)

	if err != nil {
		return err
	}
	err = utils.StartContainer(&e.Cli, cid)

	if err != nil {
		return err
	}

	err = utils.Exec(&e.Cli, cid, "mkdir /arrebol")

	if err != nil {
		log.Println("Error on creating /arrebol folder")
		return err
	}

	taskScriptExecutorPath := os.Getenv("BIN_PATH") + "/" + TaskScriptExecutorFileName

	err = utils.Copy(&e.Cli, cid, taskScriptExecutorPath, "/arrebol/"+TaskScriptExecutorFileName)

	e.Cid = cid
	return err
}

//It sends the task's commands to a file
//inside the container.
//Params:
//task - the task whose commands will be sent
//It returns:
//1. an error if the task commands couldn't be sent
//2. nil if no error happened
func (e *TaskExecutor) send(task *Task) error {
	taskScriptFileName := "task-id.ts"
	rawCmdsStr := []string{}
	for i := 0; i < len(task.Commands); i++ {
		rawCmdsStr = append(rawCmdsStr, task.Commands[i].RawCommand)
	}
	err := utils.Write(&e.Cli, e.Cid, rawCmdsStr, "/arrebol/"+taskScriptFileName)
	return err
}

func (e *TaskExecutor) run(taskId string) error {
	taskScriptFilePath := "/arrebol/task-id.ts"
	cmd := fmt.Sprintf(RunTaskScriptCommandPattern, "/arrebol/"+TaskScriptExecutorFileName, taskScriptFilePath)
	err := utils.Exec(&e.Cli, e.Cid, cmd)
	return err
}

//Tracks the task execution by counting
//how many commands have already been executed.
//It returns:
//1. 0 and an error, if it couldn't access the .ec file in the container
//2. The amount of executed commands and nil.
func (e *TaskExecutor) Track() (int, error) {
	err := utils.Exec(&e.Cli, e.Cid, "touch /arrebol/task-id.ts.ec")

	if err != nil {
		log.Println(err)
	}

	ec, err := e.getExitCodes()

	if err != nil {
		log.Println(err)
		return 0, err
	}

	return len(ec), nil
}

func (e *TaskExecutor) getExitCodes() ([]int8, error) {
	ecFilePath := "/arrebol/task-id" + ".ts.ec"
	dat, err := utils.Read(&e.Cli, e.Cid, ecFilePath)
	if err != nil {
		return nil, err
	}
	dat = bytes.TrimFunc(dat, isNotUTFNumber)
	content := string(dat[:])
	log.Println("Content: " + content)
	exitCodesStr := strings.Split(content, "\r\n")
	log.Println("ExitCodes String Array: ", exitCodesStr)
	exitCodes := toIntArray(exitCodesStr)
	return exitCodes, nil
}

func toIntArray(strs []string) []int8 {
	ints := make([]int8, 0)
	for _, s := range strs {
		x, err := strconv.Atoi(s)
		if err == nil {
			ints = append(ints, int8(x))
		}
	}
	return ints
}

func isNotUTFNumber(r rune) bool {
	if r >= 48 && r <= 57 {
		return false
	}
	return true
}
