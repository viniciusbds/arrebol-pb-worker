package worker

import (
	"bytes"
	"encoding/json"
	"github.com/ufcg-lsd/arrebol-pb-worker/utils"
	"testing"
)

var (
	workerTestInstance = Worker{
		Vcpu:    1,
		Ram:     3,
		Token:   "test-token",
		Id:      "1023",
		QueueId: "0932",
	}
)

func TestParseWorkerConfiguration(t *testing.T) {
	testingWorkerAsByte, err := json.Marshal(workerTestInstance)

	if err != nil {
		t.Errorf("Error on bytefying test worker")
	}

	parsedWorker := ParseWorkerConfiguration(bytes.NewReader(testingWorkerAsByte))

	if parsedWorker != workerTestInstance {
		t.Errorf("The parsed worked is different from the expected one")
	}
}

func TestHandleSubscriptionResponse(t *testing.T) {
	body := make(map[string]string)
	body["arrebol-worker-token"] = "test-token"
	body["queue_id"] = "192038"

	bodyAsByte, _ := json.Marshal(body)

	//exercise
	HandleJoinResponse(&utils.HttpResponse{Body: bodyAsByte, StatusCode: 201}, &workerTestInstance)

	//verification
	if workerTestInstance.QueueId != "192038" {
		t.Errorf("QueueId is not the expected one")
	}

	if workerTestInstance.Token != "test-token" {
		t.Errorf("The token is not the expected one")
	}
}
