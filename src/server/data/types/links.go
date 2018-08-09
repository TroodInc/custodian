package types

import "server/meta"

type ALink struct {
	Field   *object.FieldDescription
	IsOuter bool
	Obj     map[string]interface{}
}

type DLink struct {
	Field   *object.FieldDescription
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
