package transactions

import (
	"server/object/description"
)

type State int

const (
	Pending    State = 0
	Committed  State = 1
	RolledBack State = 2
)

type MetaDescriptionTransaction interface {
	AddCreatedMetaName(metaName string)
	CreatedMetaNameList() []string
	InitialMetaList() []*description.MetaDescription
	SetState(state State)
	State() State
}

type DbTransaction interface {
	Execute([]Operation) error
	Complete() error
	Close() error
	Transaction() interface{}
}

type GlobalTransaction struct {
	MetaDescriptionTransaction MetaDescriptionTransaction
	DbTransaction              DbTransaction
}
