package webhdfs

// FileStatus defines FILESTATUS response body.
type FileStatus struct {
	PathSuffix string `json:"pathSuffix"`
	Type       string `json:"type"`
	Length     int64  `json:"length"`
}

type fileStatusResponse struct {
	FileStatus FileStatus `json:"FileStatus"`
}

type listStatusResponse struct {
	FileStatuses struct {
		FileStatus []FileStatus `json:"FileStatus"`
	} `json:"FileStatuses"`
}
