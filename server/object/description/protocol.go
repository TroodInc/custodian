package description

import "encoding/json"

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

var (
	REST = protocol_iota("REST")
	TEST = protocol_iota("TEST")
)

func (p *Protocol) MarshalJSON() ([]byte, error) {
	if s, ok := p.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, NewMetaDescriptionError("", "json_marshal", ErrJsonMarshal, "Incorrect protocol: %v", p)
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
		return NewMetaDescriptionError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect protocol: %s", s)
	}
}
