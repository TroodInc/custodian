package meta

type FileTransaction struct {
	State                int
	InitialMetaList      *[] *MetaDescription
	CreatedMetaListNames []string
}

type DbTransaction interface {
}

type MetaTransaction struct {
	FileTransaction *FileTransaction
	DbTransaction   *DbTransaction
}
