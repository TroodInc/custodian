package types

const (
	GenericInnerLinkObjectKey = "_object"
)

type GenericInnerLink struct {
	ObjectName string
	Pk         string
	PkName     string
}

func (genericInnerLink *GenericInnerLink) AsMap() map[string]string {
	return map[string]string{GenericInnerLinkObjectKey: genericInnerLink.ObjectName, genericInnerLink.PkName: genericInnerLink.Pk}
}
