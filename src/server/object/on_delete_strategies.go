package object

import (
	"fmt"
)

type OnDeleteStrategyError struct {
	msg string
}

func (e *OnDeleteStrategyError) Error() string {
	return fmt.Sprintf("OnDelete strategy parsing error: %s", e.msg)
}

type OnDeleteStrategy int

const (
	OnDeleteCascade    OnDeleteStrategy = iota + 1
	OnDeleteRestrict
	OnDeleteSetDefault
	OnDeleteSetNull
	OnDeleteUndefined
)

const (
	OnDeleteCascadeDb    = "CASCADE"
	OnDeleteRestrictDb   = "RESTRICT"
	OnDeleteSetDefaultDb = "SET DEFAULT"
	OnDeleteSetNullDb    = "SET NULL"
	OnDeleteUndefinedDb  = "UNDEFINED"
)

const (
	OnDeleteCascadeVerbose    = "cascade"
	OnDeleteRestrictVerbose   = "restrict"
	OnDeleteSetDefaultVerbose = "setDefault"
	OnDeleteSetNullVerbose    = "setNull"
)

func (onDeleteStrategy OnDeleteStrategy) ToDbValue() string {
	switch onDeleteStrategy {
	case OnDeleteCascade:
		return OnDeleteCascadeDb
	case OnDeleteSetNull:
		return OnDeleteSetNullDb
	case OnDeleteRestrict:
		return OnDeleteRestrictDb
	case OnDeleteSetDefault:
		return OnDeleteSetDefaultDb
	default:
		return OnDeleteUndefinedDb
	}
}
func (onDeleteStrategy OnDeleteStrategy) ToVerbose() string {
	switch onDeleteStrategy {
	case OnDeleteCascade:
		return OnDeleteCascadeVerbose
	case OnDeleteSetNull:
		return OnDeleteSetNullVerbose
	case OnDeleteRestrict:
		return OnDeleteRestrictVerbose
	case OnDeleteSetDefault:
		return OnDeleteSetDefaultVerbose
	default:
		return ""
	}
}

func GetOnDeleteStrategyByVerboseName(strategyName string) (OnDeleteStrategy, error) {
	switch strategyName {
	case OnDeleteCascadeVerbose, "":
		return OnDeleteCascade, nil
	case OnDeleteSetNullVerbose:
		return OnDeleteSetNull, nil
	case OnDeleteRestrictVerbose:
		return OnDeleteRestrict, nil
	case OnDeleteSetDefaultVerbose:
		return OnDeleteSetDefault, nil
	default:
		return OnDeleteUndefined, &OnDeleteStrategyError{msg: fmt.Sprintf("Incorrect strategy %s specified", strategyName)}
	}
}

func GetOnDeleteStrategyByDbCode(strategyCode string) OnDeleteStrategy {
	switch strategyCode {
	case "c":
		return OnDeleteCascade
	case "n":
		return OnDeleteSetNull
	case "r":
		return OnDeleteRestrict
	case "d":
		return OnDeleteSetDefault
	default:
		//it should never happen
		return OnDeleteUndefined
	}
}
