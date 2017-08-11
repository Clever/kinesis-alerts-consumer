package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessMessage(t *testing.T) {
	consumer := AlertsConsumer{}
	rawmsg := "todo"
	msg, tags, err := consumer.ProcessMessage([]byte(rawmsg))
	assert.Equal(t, rawmsg, string(msg))
	assert.NoError(t, err)
	expectedTags := []string{"default"}
	assert.Equal(t, expectedTags, tags)
}

func TestSendBatch(t *testing.T) {
	input1 := []byte(`todo`)
	input2 := []byte(`todo2`)
	batchedInput := [][]byte{input1, input2}

	consumer := AlertsConsumer{}

	t.Log("verify can SendBatch to 'tag'")
	err := consumer.SendBatch(batchedInput, "tag")
	assert.NoError(t, err)
}
