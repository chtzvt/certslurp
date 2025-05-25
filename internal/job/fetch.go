package job

type FetchConfig struct {
	BatchSize  int   `json:"batch_size"`
	Workers    int   `json:"workers"`
	IndexStart int64 `json:"index_start"`
	IndexEnd   int64 `json:"index_end"` // Non-inclusive; 0 = end of log
}
