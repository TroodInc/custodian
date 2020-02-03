package v2_meta

import (
	"github.com/getlantern/deepcopy"
	"server/data/notifications"
	. "server/object/description"
	"server/transactions"
)

//Object metadata description.
type V2Meta struct {
	Name    string   `json:"name"`
	Key     string 	 `json:"key"`

	Fields  []*Field                `json:"fields"`
	Actions []*notifications.Action `json:"actions,omitempty"`

	Cas     bool     `json:"cas"`
}

func (m *V2Meta) Clone() *V2Meta {
	metaDescription := new(V2Meta)
	deepcopy.Copy(metaDescription, m)
	return metaDescription
}

func (m *V2Meta) FindAction(actionName string) *notifications.Action {
	for i, action := range m.Actions {
		if action.Name == actionName {
			return m.Actions[i]
		}
	}
	return nil
}

func (m *V2Meta) ForExport() V2Meta {
	metaCopy := V2Meta{}
	deepcopy.Copy(&metaCopy, *m)
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

func NewV2Meta(name string, key string, fields []*Field, actions []*notifications.Action, cas bool) *V2Meta {
	return &V2Meta{Name: name, Key: key, Fields: fields, Actions: actions, Cas: cas}
}

func (m *V2Meta) AddField(field *Field) *Field {
	m.Fields = append(m.Fields, field)
	return nil
}

//Returns a list of fields which are presented in the DB
func (m *V2Meta) TableFields() []*Field {
	fields := make([]*Field, 0)
	l := len(m.Fields)
	for i := 0; i < l; i++ {
		if m.Fields[i].LinkType != LinkTypeOuter && m.Fields[i].Type != FieldTypeObjects {
			fields = append(fields, m.Fields[i])
		}
	}
	return fields
}

//func (f *meta.FieldDescription) canBeLinkTo(m *V2Meta) bool {
//	isSimpleFieldWithSameTypeAsPk := f.IsSimple() && f.Type == m.Key.Type
//	isInnerLinkToMeta := f.Type == FieldTypeObject && f.LinkMeta.Name == m.Name && f.LinkType == LinkTypeInner
//	isGenericInnerLinkToMeta := f.Type == FieldTypeGeneric && f.LinkType == LinkTypeInner && utils.Contains(f.Field.LinkMetaList, m.Name)
//	canBeLinkTo := isSimpleFieldWithSameTypeAsPk || isInnerLinkToMeta || isGenericInnerLinkToMeta
//	return canBeLinkTo
//}

/*
   MetaDescription driver interface.
*/

type V2MetaDescriptionSyncer interface {
	List() ([]*V2Meta, bool, error)
	Get(name string) (*V2Meta, bool, error)
	Create(fileTransaction transactions.MetaDescriptionTransaction, m V2Meta) error
	Remove(name string) (bool, error)
	Update(name string, m V2Meta) (bool, error)
}

type MetaDbSyncer interface {
	CreateObj(transactions.DbTransaction, *V2Meta, V2MetaDescriptionSyncer) error
	RemoveObj(transactions.DbTransaction, string, bool) error
	UpdateObj(transactions.DbTransaction, *V2Meta, *V2Meta, V2MetaDescriptionSyncer) error
	UpdateObjTo(transactions.DbTransaction, *V2Meta, V2MetaDescriptionSyncer) error
	ValidateObj(transactions.DbTransaction, *V2Meta, V2MetaDescriptionSyncer) (bool, error)
	BeginTransaction() (transactions.DbTransaction, error)
	CommitTransaction(transactions.DbTransaction) (error)
	RollbackTransaction(transactions.DbTransaction) (error)
}