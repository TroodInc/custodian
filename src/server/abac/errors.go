package abac

type AccessError struct {
	s string
}

func (this *AccessError) Error() string {
	return this.s
}

func (this *AccessError) Serialize() map[string]string {
	return map[string]string{
		"code": "403",
		"msg":  this.s,
	}
}

func NewError(text string) error {
	return &AccessError{text}
}

type FilterValidationError struct {
	msg string
}

func (e *FilterValidationError) Error() string {
	return e.msg
}

func NewFilterValidationError(msg string) *FilterValidationError {
	return &FilterValidationError{msg: msg}
}
