package description

import (
	"encoding/json"
	"crypto/md5"
)

type Action struct {
	Method          Method                 `json:"method"`
	Protocol        Protocol               `json:"protocol"`
	Args            []string               `json:"args,omitempty"`
	ActiveIfNotRoot bool                   `json:"activeIfNotRoot"`
	IncludeValues   map[string]interface{} `json:"includeValues"`
}

func (action *Action) GetUid() string {
	arrBytes := []byte{}
	jsonBytes, _ := json.Marshal([]interface{}{action.Method, action.Protocol, action.Args})
	arrBytes = append(arrBytes, jsonBytes...)
	bytesResult := md5.Sum(arrBytes)
	return string(bytesResult[:])
}
