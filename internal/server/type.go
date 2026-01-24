package server

type PutReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetResp struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type PutResp struct {
	OK bool `json:"ok"`
}

type DeleteResp struct {
	OK bool `json:"ok"`
}
