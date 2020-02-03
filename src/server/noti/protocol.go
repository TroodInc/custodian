package noti

import (
	"encoding/json"
	"fmt"
	"server/errors"
)

var protocols []string

type Protocol int

func (p Protocol) String() (string, bool) {
	if i := int(p); i <= 0 || i > len(protocols) {
		return "", false
	} else {
		return protocols[i-1], true
	}
}
func protocol_iota(s string) Protocol {
	protocols = append(protocols, s)
	return Protocol(len(protocols))
}

func asProtocol(name string) (Protocol, bool) {
	for i, _ := range protocols {
		if protocols[i] == name {
			return Protocol(i + 1), true
		}
	}
	return Protocol(0), false
}

func (p *Protocol) MarshalJSON() ([]byte, error) {
	if s, ok := p.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, errors.NewValidationError("json_marshal", fmt.Sprintf("Incorrect protocol: %v", p), nil)
	}
}
func (p *Protocol) UnmarshalJSON(b []byte) error {
	var s string
	if e := json.Unmarshal(b, &s); e != nil {
		return e
	}
	if protocol, ok := asProtocol(s); ok {
		*p = protocol
		return nil
	} else {
		return errors.NewValidationError("json_unmarshal", fmt.Sprintf("Incorrect protocol: %s", s), nil)
	}
}
