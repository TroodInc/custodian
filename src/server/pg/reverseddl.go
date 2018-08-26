package pg

import (
	"database/sql"
	"fmt"

	"server/object/description"
)

type Reverser struct {
	tx    *sql.Tx
	table string
	oid   int64
}

const (
	SQL_GET_OBJ_ID string = `
	    SELECT c.oid  
	    FROM pg_catalog.pg_class c
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	    WHERE c.relname = $1 
		  AND pg_catalog.pg_table_is_visible(c.oid)
	`
	SQL_COLUMNS_DESC string = `
	    SELECT a.attname,
		    pg_catalog.format_type(a.atttypid, a.atttypmod),
		    (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
			FROM pg_catalog.pg_attrdef d
			WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) as defval,
		    a.attnotnull
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum		
	`
	SQL_PU_CONSTRAINTS string = `
	    SELECT a.attname, con.contype
		FROM pg_catalog.pg_class c, pg_catalog.pg_attribute a, 
		    pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con 
					    ON (conrelid = i.indrelid 
						AND conindid = i.indexrelid 
						AND contype IN ('p','u'))
		WHERE c.oid = $1 AND c.oid = i.indrelid
		      AND c.oid = a.attrelid AND a.attnum > 0 AND NOT a.attisdropped
		      AND a.attnum = ANY(con.conkey)
	`

	SQL_OFK_TO_TABLE string = `
	    SELECT conrelid::pg_catalog.regclass, 
		    (select a.attname from pg_catalog.pg_attribute a where c.conrelid = a.attrelid
			   AND a.attnum > 0 AND NOT a.attisdropped AND a.attnum = ANY(c.conkey)),
		    (select a.attname from pg_catalog.pg_attribute a where c.confrelid = a.attrelid
			    AND a.attnum > 0 AND NOT a.attisdropped AND a.attnum = ANY(c.confkey))
		FROM pg_catalog.pg_constraint c
		WHERE c.confrelid = $1 AND c.contype = 'f'
	`

	SQL_IFK_TO_TABLE string = `
	    SELECT (select a.attname from pg_catalog.pg_attribute a where c.conrelid = a.attrelid
			AND a.attnum > 0 AND NOT a.attisdropped AND a.attnum = ANY(c.conkey)) as conkey,
		   c.confrelid::pg_catalog.regclass,
		   (select a.attname from pg_catalog.pg_attribute a where c.confrelid = a.attrelid
			AND a.attnum > 0 AND NOT a.attisdropped AND a.attnum = ANY(c.confkey)) as confkey,
			c.confdeltype
		FROM pg_catalog.pg_constraint c
		WHERE c.conrelid = $1 AND c.contype = 'f';
	`
)

//NewReverser create a new Reversers. Returns errors if some error has occurred.
//For example, iIf the table not exists error code will be ErrNotFound.
func NewReverser(tx *sql.Tx, table string) (*Reverser, error) {
	reverser := &Reverser{tx: tx, table: table}
	err := tx.QueryRow(SQL_GET_OBJ_ID, table).Scan(&reverser.oid)
	if err == sql.ErrNoRows {
		return nil, &DDLError{table: table, code: ErrNotFound, msg: err.Error()}
	}
	if err != nil {
		return nil, &DDLError{table: table, code: ErrInternal, msg: err.Error()}
	}
	return reverser, nil
}

//Revers columns and primary key of the table.
func (r *Reverser) Columns(cols *[]Column, pk *string) error {
	colrows, err := r.tx.Query(SQL_COLUMNS_DESC, r.oid)
	if err != nil {
		return &DDLError{table: r.table, code: ErrInternal, msg: "select column desc: " + err.Error()}
	}
	defer colrows.Close()

	var column, dbtype, defval string
	var dbdefval sql.NullString
	var notnull, ok bool
	var coltyp ColumnType
	var colsmap = make(map[string]int)
	for i := 0; colrows.Next(); i++ {
		if err = colrows.Scan(&column, &dbtype, &dbdefval, &notnull); err != nil {
			return &DDLError{table: r.table, code: ErrInternal, msg: "parse column desc" + err.Error()}
		}
		if coltyp, ok = dbTypeToColumnType(dbtype); !ok {
			return &DDLError{table: r.table, code: ErrInternal, msg: fmt.Sprintf("Unknown database type: '%s'", dbtype)}
		}
		if dbdefval.Valid {
			defval = dbdefval.String
		} else {
			defval = ""
		}
		//TODO: implement this: *cols = append(*cols, Column{Name: column, Typ: coltyp, Optional: len(defval) > 0 || !notnull, Defval: defval})
		//when invariants` restrictions would be implemented (TB-116)
		*cols = append(*cols, Column{Name: column, Typ: coltyp, Optional: !notnull, Defval: defval})
		colsmap[column] = i
	}

	conrows, err := r.tx.Query(SQL_PU_CONSTRAINTS, r.oid)
	if err != nil {
		return &DDLError{table: r.table, code: ErrInternal, msg: "select PK and UK: " + err.Error()}
	}
	defer conrows.Close()

	var contyp string
	var i int
	for conrows.Next() {
		if err = conrows.Scan(&column, &contyp); err != nil {
			return &DDLError{table: r.table, code: ErrInternal, msg: "parse PK and UK:" + err.Error()}
		}
		if i, ok = colsmap[column]; !ok {
			return &DDLError{table: r.table, code: ErrInternal, msg: fmt.Sprintf("Unknown column: '%s'", column)}
		}

		switch contyp {
		case "p":
			*pk = column
			(*cols)[i].Unique = true
		case "u":
			(*cols)[i].Unique = true
		default:
			return &DDLError{table: r.table, code: ErrInternal, msg: fmt.Sprintf("Unknown constrain type: '%s'", contyp)}

		}
	}

	return nil
}

func (r *Reverser) Constraints(ifks *[]IFK, ofks *[]OFK) error {
	ofkrows, err := r.tx.Query(SQL_OFK_TO_TABLE, r.oid)
	if err != nil {
		return &DDLError{table: r.table, code: ErrInternal, msg: "select OFK: " + err.Error()}
	}
	defer ofkrows.Close()

	for ofkrows.Next() {
		var fromTbl, fromCol, toCol string
		if err = ofkrows.Scan(&fromTbl, &fromCol, &toCol); err != nil {
			return &DDLError{table: r.table, code: ErrInternal, msg: "parse OFK: " + err.Error()}
		}

		*ofks = append(*ofks, OFK{fromTbl, fromCol, toCol, r.table})
	}

	ifkrows, err := r.tx.Query(SQL_IFK_TO_TABLE, r.oid)
	if err != nil {
		return &DDLError{table: r.table, code: ErrInternal, msg: "select IFK: " + err.Error()}
	}
	defer ifkrows.Close()

	for ifkrows.Next() {
		var fromCol, toTbl, toCol, OnDeleteStrategyCode string
		if err = ifkrows.Scan(&fromCol, &toTbl, &toCol, &OnDeleteStrategyCode); err != nil {
			return &DDLError{table: r.table, code: ErrInternal, msg: "parse IFK:" + err.Error()}
		}
		*ifks = append(*ifks, IFK{fromCol, toTbl, toCol, description.GetOnDeleteStrategyByDbCode(OnDeleteStrategyCode).ToDbValue(), ""})
	}

	return nil
}
