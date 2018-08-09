package description


//The shadow struct of the Meta struct.
type MetaDescription struct {
	Name    string   `json:"name"`
	Key     string   `json:"key"`
	Fields  []Field  `json:"fields"`
	Actions []Action `json:"ActionSet,omitempty"`
	Cas     bool     `json:"cas"`
}
