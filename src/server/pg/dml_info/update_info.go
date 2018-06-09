package dml_info

type updateInfo struct {
	Table   string
	Cols    []string
	Values  []string
	Filters []string
}

func NewUpdateInfo(table string, columns []string, values []string, filters []string, ) *updateInfo {
	sqlHelper := SqlHelper{}
	return &updateInfo{table, sqlHelper.EscapeColumns(columns), values, filters}
}
