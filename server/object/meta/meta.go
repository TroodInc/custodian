package meta

import (
	"encoding/json"
	"custodian/server/noti"
	"custodian/utils"
	. "custodian/server/object/description"
	"custodian/server/transactions"
)

var notifierFactories = map[Protocol]noti.Factory{
	REST: noti.NewRestNotifier,
	TEST: noti.NewTestNotifier,
}

//Object metadata description.
type Meta struct {
	*MetaDescription
	Key       *FieldDescription
	Fields    []FieldDescription
	ActionSet *ActionSet
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

//Returns a list of fields which are presented in the DB
func (m *Meta) TableFields() []*FieldDescription {
	fields := make([]*FieldDescription, 0)
	l := len(m.Fields)
	for i := 0; i < l; i++ {
		if m.Fields[i].LinkType != LinkTypeOuter && m.Fields[i].Type != FieldTypeObjects {
			fields = append(fields, &m.Fields[i])
		}
	}
	return fields
}

func (m *Meta) MarshalJSON() ([]byte, error) {
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
   MetaDescription driver interface.
*/

type MetaDescriptionSyncer interface {
	List() ([]*MetaDescription, bool, error)
	Get(name string) (*MetaDescription, bool, error)
	Create(m MetaDescription) error
	Remove(name string) (bool, error)
	Update(name string, m MetaDescription) (bool, error)
}

type MetaDbSyncer interface {
	CreateObj(*transactions.GlobalTransactionManager, *MetaDescription, MetaDescriptionSyncer) error
	RemoveObj(*transactions.GlobalTransactionManager, string, bool) error
	UpdateObj(*transactions.GlobalTransactionManager, *MetaDescription, *MetaDescription, MetaDescriptionSyncer) error
	UpdateObjTo(transactions.DbTransaction, *MetaDescription, MetaDescriptionSyncer) error
	ValidateObj(transactions.DbTransaction, *MetaDescription, MetaDescriptionSyncer) (bool, error)
	BeginTransaction() (transactions.DbTransaction, error)
	CommitTransaction(transactions.DbTransaction) (error)
	RollbackTransaction(transactions.DbTransaction) (error)
}
