package object

import (
	"server/object/meta"
	"server/transactions"
	"text/template"
	"errors"
	"database/sql"
	"logger"
	"server/migrations/operations/object"
	"server/pg"
	"fmt"
)

type CreateObjectOperation struct {
	object.CreateObjectOperation
}

func (o *CreateObjectOperation) SyncDbDescription(metaObj *meta.Meta, transaction transactions.DbTransaction) (err error) {
	tx := transaction.Transaction().(*sql.Tx)
	var metaDdl *pg.MetaDDL
	if metaDdl, err = new(pg.MetaDdlFactory).Factory(metaObj); err != nil {
		return err
	}

	var statementSet = pg.DdlStatementSet{}
	for i, _ := range metaDdl.Seqs {
		if statement, err := metaDdl.Seqs[i].CreateDdlStatement(); err != nil {
			return err
		} else {
			statementSet.Add(statement)
		}
	}
	if statement, err := metaDdl.CreateTableDdlStatement(); err != nil {
		return err
	} else {
		statementSet.Add(statement)
	}

	for _, statement := range statementSet {
		logger.Debug("Creating object in DB: %syncer\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(metaObj.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}
	return nil
}

//Auxilary template functions
var ddlFuncs = template.FuncMap{"dict": dictionary}

//DDL create table templates
func dictionary(values ...interface{}) (map[string]interface{}, error) {
	if len(values)&1 != 0 {
		return nil, errors.New("count of arguments must be even")
	}
	dict := make(map[string]interface{}, len(values)>>1)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, errors.New("dictionary key must be of string type")
		}
		dict[key] = values[i+1]
	}
	return dict, nil
}

const (
	createTableTemplate = `CREATE TABLE "{{.Table}}" (
	{{range .Columns}}
		{{template "column" .}},{{"\n"}}
	{{end}}

	{{$mtable:=.Table}}

	{{range .IFKs}}
		{{template "ifk" dict "Mtable" $mtable "dot" .}},{{"\n"}}
	{{end}}
	
	PRIMARY KEY ("{{.Pk}}")
    );`
	columnsSubTemplate = `{{define "column"}}"{{.Name}}" {{.Typ.DdlType}}{{if not .Optional}} NOT NULL{{end}}{{if .Unique}} UNIQUE{{end}}{{if .Defval}} DEFAULT {{.Defval}}{{end}}{{end}}`
	InnerFKSubTemplate = `{{define "ifk"}}
		CONSTRAINT fk_{{.dot.FromColumn}}_{{.dot.ToTable}}_{{.dot.ToColumn}} 
		FOREIGN KEY ("{{.dot.FromColumn}}") 
		REFERENCES "{{.dot.ToTable}}" ("{{.dot.ToColumn}}") 
		ON DELETE {{.dot.OnDelete}} 
			{{if eq .dot.OnDelete "SET DEFAULT" }} {{ .dot.Default }} {{end}}
		{{end}}`
)

var parsedTemplate = template.Must(
	template.Must(
		template.Must(template.New("create_table").Funcs(ddlFuncs).Parse(createTableTemplate),
		).Parse(columnsSubTemplate)).Parse(InnerFKSubTemplate),
)

func NewCreateObjectOperation() *CreateObjectOperation {
	return &CreateObjectOperation{object.CreateObjectOperation{}}
}
