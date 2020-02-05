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
	FieldDescription *meta.Field
	PkName           string
}

func (genericInnerLink *GenericInnerLink) AsMap() map[string]interface{} {
	return map[string]interface{}{GenericInnerLinkObjectKey: genericInnerLink.ObjectName, genericInnerLink.PkName: genericInnerLink.Pk}
}

type AGenericInnerLink struct {
	GenericInnerLink *GenericInnerLink
	Field            *meta.Field
	RecordData       map[string]interface{} //used as a stash to access after operations with record this link is pointing to
	Index            int                    //Index and NeighboursCount are used to restore original order of related records,
	NeighboursCount  int                    // passed to processing
	LinkType         meta.LinkType
}
