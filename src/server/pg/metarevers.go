package pg

import (
	"database/sql"
	"server/object"
)

func ReverseMeta(name string, tx *sql.Tx) (*object.Meta, error) {
	resultMeta := &object.Meta{}

	var tableOID int64
	if err := tx.QueryRow(SQL_GET_OBJ_ID, TableNamePrefix + name).Scan(&tableOID); err != nil {
		return nil, &DDLError{table: TableNamePrefix + name, code: ErrInternal, msg: err.Error()}
	}

	//constraints, _ := GetTableConstraints(tableOID, tx)

	if colrows, err := tx.Query(SQL_COLUMNS_DESC, tableOID); err != nil {
		return nil, &DDLError{table: TableNamePrefix + name, code: ErrInternal, msg: err.Error()}
	} else {
		for i := 0; colrows.Next(); i++ {
			var column, dbtype, defval string
			var notnull bool

			colrows.Scan(&column, &dbtype, &defval, &notnull)

			//resultMeta.Fields = append(resultMeta.Fields, description.Field{
			//	Name:           column,
			//	Type:           0,
			//	Optional:       false,
			//	Unique:         false,
			//	Def:            defval,
			//})
		}
	}

	return resultMeta, nil
}

func GetTableConstraints(tableOID int64, tx *sql.Tx) (map[string]string, error) {
	var columns = make(map[string]string)

	if conrows, err := tx.Query(SQL_PU_CONSTRAINTS, tableOID); err != nil {
		return nil, &DDLError{table: "<change_me>", code: ErrInternal, msg: "select PK and UK: " + err.Error()}
	} else {
		defer conrows.Close()

		for conrows.Next() {
			var column, contyp string
			conrows.Scan(&column, &contyp)

			columns[column] = contyp
		}
	}

	return columns, nil
}