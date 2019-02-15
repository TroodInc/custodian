package description

type Action struct {
	Method          Method                 `json:"method"`
	Protocol        Protocol               `json:"protocol"`
	Args            []string               `json:"args,omitempty"`
	ActiveIfNotRoot bool                   `json:"activeIfNotRoot"`
	IncludeValues   map[string]interface{} `json:"includeValues"`
	Name            string                 `json:"name"`
	id              int
}

func (a *Action) Clone() *Action {
	return &Action{
		Method:          a.Method,
		Protocol:        a.Protocol,
		Args:            a.Args,
		ActiveIfNotRoot: a.ActiveIfNotRoot,
		IncludeValues:   a.IncludeValues,
		Name:            a.Name,
	}
}

func (a *Action) SetId(id int) {
	a.id = id
}

func (a *Action) Id() int {
	return a.id
}
