package abac

// AccessError structure
type AccessError struct {
	s string
}

// Error function
func (error *AccessError) Error() string {
	return error.s
}

// Serialize access error
func (error *AccessError) Serialize() map[string]string {
	return map[string]string{
		"code": "403",
		"msg":  error.s,
	}
}

// NewError function
func NewError(text string) error {
	return &AccessError{text}
}

// FilterValidationError function
type FilterValidationError struct {
	msg string
}

func (e *FilterValidationError) Error() string {
	return e.msg
}

// NewFilterValidationError function
func NewFilterValidationError(msg string) *FilterValidationError {
	return &FilterValidationError{msg: msg}
}
