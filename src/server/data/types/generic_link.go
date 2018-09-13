package types

import (
	"server/object/meta"
)

const (
	GenericInnerLinkObjectKey = "_object"
)

type GenericInnerLink struct {
	ObjectName       string
	Pk               interface{}
	FieldDescription *meta.FieldDescription
	PkName           string
	RecordData       map[string]interface{} //used as a stash to access after operations with record this link is pointing to
}

func (genericInnerLink *GenericInnerLink) AsMap() map[string]interface{} {
	return map[string]interface{}{GenericInnerLinkObjectKey: genericInnerLink.ObjectName, genericInnerLink.PkName: genericInnerLink.Pk}
}
