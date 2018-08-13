package pg

import (
	"server/data"
)

type ExecuteContext struct {
	Tx *Tx
}

func (ex *ExecuteContext) Execute(ops []data.Operation) error {
	ctx := &pgOpCtx{tx: ex.Tx}
	for _, op := range ops {
		if err := op(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (ex *ExecuteContext) Complete() error {
	if err := ex.Tx.Commit(); err != nil {
		return NewDMLError(ErrCommitFailed, err.Error())
	}
	return nil
}

func (ex *ExecuteContext) Close() error {
	return ex.Tx.Rollback()
}
