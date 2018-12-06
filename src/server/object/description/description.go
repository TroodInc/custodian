package description

//The shadow struct of the Meta struct.
type MetaDescription struct {
	Name    string   `json:"name"`
	Key     string   `json:"key"`
	Fields  []Field  `json:"fields"`
	Actions []Action `json:"actions,omitempty"`
	Cas     bool     `json:"cas"`
}

func NewMetaDescription(name string, key string, fields []Field, actions []Action, cas bool) *MetaDescription {
	return &MetaDescription{Name: name, Key: key, Fields: fields, Actions: actions, Cas: cas}
}
