package dml_info

type UpdateInfo struct {
	Table   string
	Cols    []string
	Values  []string
	Filters []string
}
