package main

type Cmd string

const (
	CmdPublish Cmd = "publish"
	CmdConsume Cmd = "consume"
	CmdAck     Cmd = "ack"
)

type Status string

const (
	StatusOK    Status = "ok"
	StatusError Status = "error"
)

type Request struct {
	Cmd    Cmd    `json:"cmd"`
	Topic  string `json:"topic"`
	Body   string `json:"body,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Group  string `json:"group,omitempty"`
}

type Response struct {
	Status     Status    `json:"status"`
	Error      string    `json:"error,omitempty"`
	Offset     int64     `json:"offset,omitempty"`
	Messages   []Message `json:"message,omitempty"`
	NextOffset int64     `json:"nextOffset,omitempty"`
}

func groupOrDefault(req Request) string {
	if req.Group == "" {
		return "default"
	}
	return req.Group
}
