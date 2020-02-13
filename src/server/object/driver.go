package object

import "server/object/meta"

type MetaDriver interface {
	Get(name string) *meta.Meta
	List() ([]*meta.Meta, error)
	Create(m *meta.Meta) error
	Update(m *meta.Meta) (bool, error)
	Remove(name string) (bool, error)
}
