package meta

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

func (onDeleteStrategy OnDeleteStrategy) ToDbValue() string {
	switch onDeleteStrategy {
	case OnDeleteCascade:
		return "CASCADE"
	case OnDeleteSetNull:
		return "SET NULL"
	case OnDeleteRestrict:
		return "RESTRICT"
	case OnDeleteSetDefault:
		return "SET DEFAULT"
	default:
		return "UNDEFINED"
	}
}

func GetOnDeleteStrategyByVerboseName(strategyName string) (OnDeleteStrategy, error) {
	switch strategyName {
	case "cascade", "":
		return OnDeleteCascade, nil
	case "setNull":
		return OnDeleteSetNull, nil
	case "restrict":
		return OnDeleteRestrict, nil
	case "setDefault":
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
