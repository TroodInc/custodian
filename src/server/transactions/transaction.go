package transactions

type FileMetaDescriptionTransaction struct {
	createdMetaNameList []string
	state               State
	initialMetaList     []map[string]interface{}
}

func (f *FileMetaDescriptionTransaction) AddCreatedMetaName(metaName string) {
	f.createdMetaNameList = append(f.createdMetaNameList, metaName)
}

func (f *FileMetaDescriptionTransaction) CreatedMetaNameList() []string {
	return f.createdMetaNameList
}

func (f *FileMetaDescriptionTransaction) SetState(state State) {
	f.state = state
}

func (f *FileMetaDescriptionTransaction) State() State {
	return f.state
}
func (f *FileMetaDescriptionTransaction) InitialMetaList() []map[string]interface{} {
	return f.initialMetaList
}

func NewFileMetaDescriptionTransaction(state State, initialMetaList []map[string]interface{}) *FileMetaDescriptionTransaction {
	return &FileMetaDescriptionTransaction{
		createdMetaNameList: make([]string, 0),
		state:               state,
		initialMetaList:     initialMetaList,
	}
}
