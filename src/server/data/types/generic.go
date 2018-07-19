package types

const (
	GenericInnerLinkObjectKey = "_object"
	GenericPkKey              = "pk"
)

type GenericInnerLink struct {
	ObjectName string
	Pk         string
}
