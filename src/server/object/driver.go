package object

import "server/object/meta"

type MetaDriver interface {
	Get(name string) map[string]interface{}
	List() ([]map[string]interface{}, error)
	Create(m *meta.Meta) error
	Update(m *meta.Meta) (bool, error)
	Remove(name string) (bool, error)
}
