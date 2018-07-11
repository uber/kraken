package hdfsbackend

type fileStatus struct {
	PathSuffix string `json:"pathSuffix"`
	Type       string `json:"type"`
	Length     int64  `json:"length"`
}

type fileStatusResponse struct {
	FileStatus fileStatus `json:"FileStatus"`
}

type listStatusResponse struct {
	FileStatuses struct {
		FileStatus []fileStatus `json:"FileStatus"`
	} `json:"FileStatuses"`
}
