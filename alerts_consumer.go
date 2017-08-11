package main

import (
	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var lg = logger.New("kinesis-cwlogs-splitter")

type AlertsConsumer struct {
}

// ProcessMessage is called once per log to parse the log line and then reformat it
// so that it can be directly used by the output. The returned tags will be passed along
// with the encoded log to SendBatch()
func (c *AlertsConsumer) ProcessMessage(rawmsg []byte) (msg []byte, tags []string, err error) {
	return rawmsg, []string{"default"}, nil
}

// SendBatch is called once per batch per tag
func (c *AlertsConsumer) SendBatch(batch [][]byte, tag string) error {
	return nil
}
