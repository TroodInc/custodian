package dml_info

type DeleteInfo struct {
	Table   string
	Filters []string
}

func NewDeleteInfo(table string, filters []string) *DeleteInfo {
	return &DeleteInfo{table, filters}
}
