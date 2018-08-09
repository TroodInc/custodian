package types

import "server/meta"

const (
	GenericInnerLinkObjectKey = "_object"
)

type GenericInnerLink struct {
	ObjectName       string
	Pk               interface{}
	FieldDescription *object.FieldDescription
	PkName           string
}

func (genericInnerLink *GenericInnerLink) AsMap() map[string]interface{} {
	return map[string]interface{}{GenericInnerLinkObjectKey: genericInnerLink.ObjectName, genericInnerLink.PkName: genericInnerLink.Pk}
}
