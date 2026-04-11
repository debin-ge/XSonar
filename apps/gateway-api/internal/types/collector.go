package types

type CreateCollectorTaskReq struct {
	TaskID           string `json:"task_id"`
	TaskType         string `json:"task_type"`
	Keyword          string `json:"keyword"`
	Priority         int32  `json:"priority,optional"`
	FrequencySeconds *int32 `json:"frequency_seconds,optional"`
	Since            string `json:"since,optional"`
	Until            string `json:"until,optional"`
	RequiredCount    *int64 `json:"required_count,optional"`
}

type CollectorTaskPathReq struct {
	ID string `path:"id"`
}
