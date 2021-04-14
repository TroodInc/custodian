package errors

type GenericFieldPkIsNullError struct {
	Msg      string
	MetaName string
}

func (e GenericFieldPkIsNullError) Error() string {
	return ""
}
