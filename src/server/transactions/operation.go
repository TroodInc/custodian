package transactions


type Operation func(dbTransaction DbTransaction) error