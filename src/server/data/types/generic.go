package types

const (
	GENERIC_INNER_LINK_OBJECT_KEY = "_object"
	GENERIC_PK_KEY                = "pk"
)

type GenericInnerLink struct {
	ObjectName string
	Pk         string
}
