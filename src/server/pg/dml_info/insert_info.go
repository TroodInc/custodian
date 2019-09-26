package dml_info

import (
	"bytes"
)

type insertInfo struct {
	Table      string
	Cols       []string
	RCols      []string
	ObjectsLen int
}

func (insertInfo *insertInfo) GetValues() string {
	var b bytes.Buffer
	for i := 0; i < insertInfo.ObjectsLen; i++ {
		b.WriteRune('(')
		b.WriteString(BindValues(i*len(insertInfo.Cols)+1, len(insertInfo.Cols)))
		b.WriteString("),")
	}
	b.Truncate(b.Len() - 1)
	return b.String()
}

func NewInsertInfo(table string, columns []string, returnColumns []string, objectsLength int) *insertInfo {
	return &insertInfo{table, EscapeColumns(columns), EscapeColumns(returnColumns), objectsLength}
}
