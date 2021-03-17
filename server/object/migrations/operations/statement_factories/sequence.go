package statement_factories

import (
	"bytes"
	"custodian/server/object"
	"fmt"
	"text/template"
)

type SequenceStatementFactory struct{}

func (ssm *SequenceStatementFactory) FactoryCreateStatement(sequence *object.Seq) (*object.DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedCreateSequenceTemplate.Execute(&buffer, sequence); e != nil {
		return nil, object.NewDdlError(object.ErrInternal, e.Error(), sequence.Name)
	}
	return object.NewDdlStatement(fmt.Sprintf("create_seq#%s", sequence.Name), buffer.String()), nil
}

func (ssm *SequenceStatementFactory) FactoryDropStatement(sequence *object.Seq) (*object.DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedDropSequenceTemplate.Execute(&buffer, sequence); e != nil {
		return nil, object.NewDdlError(object.ErrInternal, e.Error(), sequence.Name)
	}
	return object.NewDdlStatement(fmt.Sprintf("drop_seq#%s", sequence.Name), buffer.String()), nil
}

func (ssm *SequenceStatementFactory) FactoryRenameStatement(currentSequence *object.Seq, newSequence *object.Seq) (*object.DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedRenameSequenceTemplate.Execute(&buffer, map[string]string{"CurrentName": currentSequence.Name, "NewName": newSequence.Name}); e != nil {
		return nil, e
	}
	return object.NewDdlStatement(fmt.Sprintf("alter_seq#%s", currentSequence.Name), buffer.String()), nil
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
