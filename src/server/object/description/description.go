package description

import "github.com/getlantern/deepcopy"

//The shadow struct of the Meta struct.
type MetaDescription struct {
	Name    string   `json:"name"`
	Key     string   `json:"key"`
	Fields  []Field  `json:"fields"`
	Actions []Action `json:"actions,omitempty"`
	Cas     bool     `json:"cas"`
}

func (md *MetaDescription) Clone() *MetaDescription {
	metaDescription := new(MetaDescription)
	deepcopy.Copy(metaDescription, md)
	return metaDescription
}
