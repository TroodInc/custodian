package types

import "server/object/meta"

type ALink struct {
	Field           *meta.FieldDescription
	IsOuter         bool
	Obj             map[string]interface{}
	Index           int //Index and NeighboursCount are used to restore original order of related records,
	NeighboursCount int // passed to processing
}

type DLink struct {
	Field   *meta.FieldDescription
	IsOuter bool
	Id      interface{}
}
