package file_transaction

import (
	. "custodian/server/transactions"
	"custodian/server/object/description"
)

type FileMetaDescriptionTransaction struct {
	createdMetaNameList []string
	state               State
	initialMetaList     []*description.MetaDescription
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
func (f *FileMetaDescriptionTransaction) InitialMetaList() []*description.MetaDescription {
	return f.initialMetaList
}

func NewFileMetaDescriptionTransaction(state State, initialMetaList []*description.MetaDescription) *FileMetaDescriptionTransaction {
	return &FileMetaDescriptionTransaction{
		createdMetaNameList: make([]string, 0),
		state:               state,
		initialMetaList:     initialMetaList,
	}
}
