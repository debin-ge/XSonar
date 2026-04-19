//lint:file-ignore SA5008 go-zero API types use non-standard json options.
package types

type CreatePeriodicCollectorTaskReq struct {
	TaskID           string `json:"task_id"`
	Keyword          string `json:"keyword"`
	Priority         int32  `json:"priority,optional"`
	FrequencySeconds *int32 `json:"frequency_seconds,optional"`
	PerRunCount      *int64 `json:"per_run_count,optional"`
	RequiredCount    *int64 `json:"required_count,optional"`
}

type CreateRangeCollectorTaskReq struct {
	TaskID        string `json:"task_id"`
	Keyword       string `json:"keyword"`
	Priority      int32  `json:"priority,optional"`
	Since         string `json:"since,optional"`
	Until         string `json:"until,optional"`
	RequiredCount *int64 `json:"required_count,optional"`
}

type CollectorTaskPathReq struct {
	ID string `path:"id"`
}
