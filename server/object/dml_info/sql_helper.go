package dml_info

import (
	"bytes"
	"fmt"
	"strconv"
)

func EscapeColumn(column string) string {
	return fmt.Sprintf("\"%s\"", column)
}

func EscapeColumns(columns []string) []string {
	escapedColumns := make([]string, 0)
	for _, column := range columns {
		escapedColumns = append(escapedColumns, EscapeColumn(column))
	}
	return escapedColumns
}

func BindValues(startWith int, count int) string {
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
