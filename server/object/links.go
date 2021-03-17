package object

//LazyLink links current record with related and is used to set a value of related record`s attribute.
//Eg.: object A has a link to object B called "b" and B has outer link to A called "b_set". Thus B record may have a
//LazyLink to A which will be used to fill A record`s "b_set" value with B record`s data.

type LazyLink struct {
	Field           *FieldDescription
	IsOuter         bool
	Obj             map[string]interface{}
	Index           int //Index and NeighboursCount are used to restore original order of related records,
	NeighboursCount int // passed to processing
}

type DLink struct {
	Field   *FieldDescription
	IsOuter bool
	Id      interface{}
}
