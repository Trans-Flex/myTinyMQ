package main

type GroupMeta struct {
	Offset   int64   `json:"offset"`
	AckedSet []int64 `json:"acked_set"`
}

type MetaData struct {
	Groups map[string]GroupMeta `json:"groups"`
}
