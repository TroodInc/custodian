package types

import "server/meta"

type ALink struct {
	Field   *meta.FieldDescription
	IsOuter bool
	Obj     map[string]interface{}
}

type DLink struct {
	Field   *meta.FieldDescription
	IsOuter bool
	Id      interface{}
}

func AssertLink(i interface{}) bool {
	switch i.(type) {
	case DLink:
		return true
	case ALink:
		return true
	default:
		return false
	}
}
