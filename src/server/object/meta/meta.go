package meta

import (
	"encoding/json"
	"server/noti"
	"utils"
	. "server/object/description"
	"server/transactions"
	"github.com/getlantern/deepcopy"
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

func (m *Meta) ReverseOuterField(innerFieldName string) *FieldDescription {
	innerField := m.FindField(innerFieldName)
	for _, field := range innerField.LinkMeta.Fields {
		if field.Type == FieldTypeArray && field.LinkType == LinkTypeOuter {
			if field.OuterLinkField.Name == innerField.Name && field.LinkMeta.Name == m.Name {
				return &field
			}
		}
	}
	return nil
}

func (m *Meta) AddField(fieldDescription FieldDescription) *FieldDescription {
	m.Fields = append(m.Fields, fieldDescription)
	m.MetaDescription.Fields = append(m.MetaDescription.Fields, *fieldDescription.Field)
	return nil
}

func (m *Meta) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.MetaDescription)
}

func (m *Meta) DescriptionForExport() MetaDescription {
	metaCopy := MetaDescription{}
	deepcopy.Copy(&metaCopy, *m.MetaDescription)
	for i := len(metaCopy.Fields) - 1; i >= 0; i-- {
		if metaCopy.Fields[i].LinkType == LinkTypeOuter {
			// exclude field supporting only query mode
			if !metaCopy.Fields[i].RetrieveMode {
				metaCopy.Fields = append(metaCopy.Fields[:i], metaCopy.Fields[i+1:]...)
				continue
			}
			//false value are interpreted as zero value
			metaCopy.Fields[i].RetrieveMode = false
			metaCopy.Fields[i].QueryMode = false
		}
	}
	return metaCopy
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
	Create(fileTransaction transactions.MetaDescriptionTransaction, m MetaDescription) error
	Remove(name string) (bool, error)
	Update(name string, m MetaDescription) (bool, error)
}

type MetaDbSyncer interface {
	CreateObj(transactions.DbTransaction, *Meta) error
	RemoveObj(transactions.DbTransaction, string, bool) error
	UpdateObj(transactions.DbTransaction, *Meta, *Meta) error
	UpdateObjTo(transactions.DbTransaction, *Meta) error
	ValidateObj(transactions.DbTransaction, *Meta) (bool, error)
	BeginTransaction() (transactions.DbTransaction, error)
	CommitTransaction(transactions.DbTransaction) (error)
	RollbackTransaction(transactions.DbTransaction) (error)
}
