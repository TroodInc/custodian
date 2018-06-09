package dml_info

import (
	"fmt"
	"bytes"
	"strconv"
)

type SqlHelper struct {
}

func (sqlHelper *SqlHelper) EscapeColumn(column string) string {
	return fmt.Sprintf("\"%s\"", column)
}

func (sqlHelper *SqlHelper) EscapeColumns(columns []string) []string {
	escapedColumns := make([]string, 0)
	for _, column := range columns {
		escapedColumns = append(escapedColumns, sqlHelper.EscapeColumn(column))
	}
	return escapedColumns
}

func (sqlHelper *SqlHelper) BindValues(startWith int, count int) string {
	var vals bytes.Buffer
	for i := startWith; i < startWith+count; i++ {
		vals.WriteString("$")
		vals.WriteString(strconv.Itoa(i))
		vals.WriteString(",")
	}
	if vals.Len() > 0 {
		vals.Truncate(vals.Len() - 1)
	}
	return vals.String()
}
