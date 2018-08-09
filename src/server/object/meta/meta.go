package meta

import (
	"encoding/json"
	"server/noti"
	"utils"
	. "server/meta/description"
)

type Def interface{}
type DefConstStr struct{ Value string }
type DefConstFloat struct{ Value float64 }
type DefConstInt struct{ Value int }
type DefConstBool struct{ Value bool }
type DefExpr struct {
	Func string
	Args []interface{}
}


var notifierFactories = map[Protocol]noti.Factory{
	REST: noti.NewRestNotifier,
	TEST: noti.NewTestNotifier,
}

//Object metadata description.
type Meta struct {
	*MetaDescription
	Key     *FieldDescription
	Fields  []FieldDescription
	Actions *ActionSet
}

func (m *Meta) FindField(name string) *FieldDescription {
	for i, _ := range m.Fields {
		if m.Fields[i].Name == name {
			return &m.Fields[i]
		}
	}
	return nil
}

func (m *Meta) AddField(fieldDescription FieldDescription) *FieldDescription {
	m.Fields = append(m.Fields, fieldDescription)
	m.MetaDescription.Fields = append(m.MetaDescription.Fields, *fieldDescription.Field)
	return nil
}

func (m Meta) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.MetaDescription)
}

func (f *FieldDescription) canBeLinkTo(m *Meta) bool {
	isSimpleFieldWithSameTypeAsPk := f.IsSimple() && f.Type == m.Key.Type
	isInnerLinkToMeta := f.Type == FieldTypeObject && f.LinkMeta.Name == m.Name && f.LinkType == LinkTypeInner
	isGenericInnerLinkToMeta := f.Type == FieldTypeGeneric && f.LinkType == LinkTypeInner && utils.Contains(f.Field.LinkMetaList, m.Name)
	canBeLinkTo := isSimpleFieldWithSameTypeAsPk || isInnerLinkToMeta || isGenericInnerLinkToMeta
	return canBeLinkTo
}

/*
   Meta driver interface.
*/

type MetaDriver interface {
	List() (*[]*MetaDescription, bool, error)
	Get(name string) (*MetaDescription, bool, error)
	Create(fileTransaction *FileTransaction, m MetaDescription) error
	Remove(name string) (bool, error)
	Update(name string, m MetaDescription) (bool, error)
	BeginTransaction() (*FileTransaction, error)
	CommitTransaction(transaction *FileTransaction) (error)
	RollbackTransaction(transaction *FileTransaction) (error)
}

type MetaDbSyncer interface {
	CreateObj(DbTransaction, *Meta) error
	RemoveObj(DbTransaction, string, bool) error
	UpdateObj(DbTransaction, *Meta, *Meta) error
	UpdateObjTo(DbTransaction, *Meta) error
	ValidateObj(DbTransaction, *Meta) (bool, error)
	BeginTransaction() (DbTransaction, error)
	CommitTransaction(DbTransaction) (error)
	RollbackTransaction(DbTransaction) (error)
}
