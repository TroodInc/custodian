package meta

import (
	"github.com/getlantern/deepcopy"
	"server/data/notifications"
	"server/transactions"
)

//Object metadata description.
type Meta struct {
	Name    string                  `json:"name"`
	Key     string                  `json:"key"`
	Fields  map[string]*Field                `json:"fields"`
	Actions []*notifications.Action `json:"actions,omitempty"`
	Cas     bool                    `json:"cas"`
}


func NewMeta(name string, key string, fields []*Field, actions []*notifications.Action, cas bool) *Meta {
	result := &Meta{Name: name, Key: key, Fields: make(map[string]*Field, 0), Actions: actions, Cas: cas}
	for _, field := range fields {
		result.AddField(field)
	}
	return result
}

func NewMetaFromMap(object map[string]interface{}) *Meta {
	result := &Meta{
		Name:    object["name"].(string),
		Key:     object["key"].(string),
		Fields:  make(map[string]*Field, 0),
		Actions: nil,
		Cas:     object["cas"].(bool),
	}

	switch object["fields"].(type) {
		case []map[string]interface{}:
			for _, field := range object["fields"].([]map[string]interface{}) {
				result.AddField(NewFieldFromMap(field))
			}
		case []interface{}:
			for _, field := range object["fields"].([]interface{}) {
				result.AddField(NewFieldFromMap(field.(map[string]interface{})))
			}
	}

	return result
}

func (m *Meta) Clone() *Meta {
	metaDescription := new(Meta)
	deepcopy.Copy(metaDescription, m)
	return metaDescription
}

func (m *Meta) FindAction(actionName string) *notifications.Action {
	for _, action := range m.Actions {
		if action.Name == actionName {
			return action
		}
	}
	return nil
}

func (m *Meta) ForExport() map[string]interface{} {
	result := map[string]interface{}{
		"name": m.Name,
		"key":  m.Key,
		"cas":  m.Cas,
		"fields": make([]map[string]interface{}, 0),
		"actions": make([]map[string]interface{}, 0),
	}

	for _, field := range m.Fields {
		result["fields"] = append(result["fields"].([]map[string]interface{}), field.ForExport())
	}

	return result
}

// TODO: Refactor fields storage to Map ...
func (m *Meta) GetKey() *Field {
	return m.Fields[m.Key]
}

func (m *Meta) FindField(name string) *Field {
	return m.Fields[name]
}
// <---

func (m *Meta) AddField(field *Field) error {
	if item, exist := m.Fields[field.Name]; exist {
		//TODO: Add code
		return errors.NewValidationError("", fmt.Sprintf("Field %s already exists", field.Name), item)
	}

	m.Fields[field.Name] = field

	return nil
}

//Returns a list of fields which are presented in the DB
func (m *Meta) TableFields() []*Field {
	fields := make([]*Field, 0)
	for _, field := range m.Fields{
		if field.LinkType != LinkTypeOuter && field.Type != FieldTypeObjects {
			fields = append(fields, field)
		}
	}
	return fields
}

/*
   MetaDescription driver interface.
*/

type MetaDescriptionSyncer interface {
	List() ([]map[string]interface{}, bool, error)
	Get(name string) (map[string]interface{}, bool, error)
	Create(transaction transactions.MetaDescriptionTransaction, name string, m map[string]interface{}) error
	Remove(name string) (bool, error)
	Update(name string, m map[string]interface{}) (bool, error)
}

type MetaDbSyncer interface {
	CreateObj(transactions.DbTransaction, *Meta, MetaDescriptionSyncer) error
	RemoveObj(transactions.DbTransaction, string, bool) error
	UpdateObj(transactions.DbTransaction, *Meta, *Meta, MetaDescriptionSyncer) error
	UpdateObjTo(transactions.DbTransaction, *Meta, MetaDescriptionSyncer) error
	ValidateObj(transactions.DbTransaction, *Meta, MetaDescriptionSyncer) (bool, error)
	BeginTransaction() (transactions.DbTransaction, error)
	CommitTransaction(transactions.DbTransaction) error
	RollbackTransaction(transactions.DbTransaction) error
}
