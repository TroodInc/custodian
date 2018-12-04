package field

import (
	"server/object/meta"
	"server/transactions"
	"text/template"
	"database/sql"
	"server/migrations/operations/field"
	"server/pg"
	"fmt"
	"bytes"
	"logger"
)

type AddFieldOperation struct {
	field.AddFieldOperation
}

func (o *AddFieldOperation) SyncDbDescription(metaObj *meta.Meta, transaction transactions.DbTransaction) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	columns, ifk, _, seq, err := new(pg.MetaDdlFactory).FactoryFieldProperties(o.Field)
	if err != nil {
		return err
	}
	var statementSet = pg.DdlStatementSet{}

	if err := o.addSequenceStatement(&statementSet, seq); err != nil {
		return err
	}

	if err := o.addColumnStatements(&statementSet, &columns, metaObj); err != nil {
		return err
	}

	if err := o.addConstraintStatement(&statementSet, ifk, metaObj); err != nil {
		return err
	}

	for _, statement := range statementSet {
		logger.Debug("Creating field in DB: %s\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(metaObj.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}

	return nil
}

//sequence
func (o *AddFieldOperation) addSequenceStatement(statementSet *pg.DdlStatementSet, sequence *pg.Seq) error {
	if sequence == nil {
		return nil
	}

	var buffer bytes.Buffer

	if e := parsedTemplateCreateSeq.Execute(&buffer, sequence); e != nil {
		return pg.NewDdlError(pg.ErrInternal, e.Error(), sequence.Name)
	}
	statementSet.Add(pg.NewDdlStatement(fmt.Sprintf("create_seq#%s", sequence.Name), buffer.String()))
	return nil
}

const createSequenceTemplate = `CREATE SEQUENCE "{{.Name}}";`

var parsedTemplateCreateSeq = template.Must(template.New("add_seq").Parse(createSequenceTemplate))
//

// column
func (o *AddFieldOperation) addColumnStatements(statementSet *pg.DdlStatementSet, columns *[]pg.Column, metaObj *meta.Meta) error {
	var buffer bytes.Buffer

	tableName := pg.GetTableName(metaObj.Name)
	for _, column := range *columns {
		buffer.Reset()
		if err := parsedAddTableColumnTemplate.Execute(&buffer, map[string]interface{}{
			"Table": tableName,
			"dot":   column}); err != nil {
			return pg.NewDdlError(pg.ErrExecutingDDL, err.Error(), pg.GetTableName(metaObj.Name))
		}
		name := fmt.Sprintf("add_table_column#%s.%s", tableName, column.Name)
		statementSet.Add(pg.NewDdlStatement(name, buffer.String()))
	}
	return nil
}

const addTableColumnTemplate = `ALTER TABLE "{{.Table}}" ADD COLUMN "{{.dot.Name}}" {{.dot.Typ.DdlType}}{{if not .dot.Optional}} NOT NULL{{end}}{{if .dot.Unique}} UNIQUE{{end}}{{if .dot.Defval}} DEFAULT {{.dot.Defval}}{{end}};`

var parsedAddTableColumnTemplate = template.Must(template.New("add_table_column").Parse(addTableColumnTemplate))

//Constraint
const addInnerFkConstraintTemplate = `
	ALTER TABLE "{{.Table}}" 
	ADD CONSTRAINT fk_{{.dot.FromColumn}}_{{.dot.ToTable}}_{{.dot.ToColumn}} 
	FOREIGN KEY ("{{.dot.FromColumn}}") 
	REFERENCES "{{.dot.ToTable}}" ("{{.dot.ToColumn}}") 
	ON DELETE {{.dot.OnDelete}} 
	{{if eq .dot.OnDelete "SET DEFAULT" }} 
		{{ .dot.Default }} 
	{{end}};
`

var parsedAddInnerFkConstraintTemplate = template.Must(template.New("add_ifk").Parse(addInnerFkConstraintTemplate))

func (o *AddFieldOperation) addConstraintStatement(statementSet *pg.DdlStatementSet, ifk *pg.IFK, metaObj *meta.Meta) error {
	if ifk == nil {
		return nil
	}

	var buffer bytes.Buffer
	tableName := pg.GetTableName(metaObj.Name)

	name := fmt.Sprintf("add_ifk#%s_%s_%s_%s", tableName, ifk.FromColumn, ifk.ToTable, ifk.ToColumn)

	if e := parsedAddInnerFkConstraintTemplate.Execute(&buffer, map[string]interface{}{
		"Table": tableName,
		"dot":   ifk}); e != nil {
		return pg.NewDdlError(tableName, pg.ErrInternal, e.Error())
	}
	statementSet.Add(pg.NewDdlStatement(name, buffer.String()))

	return nil
}

func NewAddFieldOperation(targetField *meta.FieldDescription) *AddFieldOperation {
	return &AddFieldOperation{field.AddFieldOperation{Field: targetField}}
}
