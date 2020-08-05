package statement_factories

import (
	"custodian/server/pg"
	"bytes"
	"fmt"
	"text/template"
)

type SequenceStatementFactory struct{}

func (ssm *SequenceStatementFactory) FactoryCreateStatement(sequence *pg.Seq) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedCreateSequenceTemplate.Execute(&buffer, sequence); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), sequence.Name)
	}
	return pg.NewDdlStatement(fmt.Sprintf("create_seq#%s", sequence.Name), buffer.String()), nil
}

func (ssm *SequenceStatementFactory) FactoryDropStatement(sequence *pg.Seq) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedDropSequenceTemplate.Execute(&buffer, sequence); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), sequence.Name)
	}
	return pg.NewDdlStatement(fmt.Sprintf("drop_seq#%s", sequence.Name), buffer.String()), nil
}

func (ssm *SequenceStatementFactory) FactoryRenameStatement(currentSequence *pg.Seq, newSequence *pg.Seq) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedRenameSequenceTemplate.Execute(&buffer, map[string]string{"CurrentName": currentSequence.Name, "NewName": newSequence.Name}); e != nil {
		return nil, e
	}
	return pg.NewDdlStatement(fmt.Sprintf("alter_seq#%s", currentSequence.Name), buffer.String()), nil
}

//Templates
const createSequenceTemplate = `CREATE SEQUENCE IF NOT EXISTS "{{.Name}}";`

var parsedCreateSequenceTemplate = template.Must(template.New("create_seq").Parse(createSequenceTemplate))
//
const dropSequenceTemplate = `DROP SEQUENCE "{{.Name}}" CASCADE;`

var parsedDropSequenceTemplate = template.Must(template.New("drop_seq").Parse(dropSequenceTemplate))
//
const renameSequenceTemplate = `ALTER SEQUENCE "{{.CurrentName}}" RENAME TO "{{.NewName}}";`

var parsedRenameSequenceTemplate = template.Must(template.New("rename_seq").Parse(renameSequenceTemplate))
