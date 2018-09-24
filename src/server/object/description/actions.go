package description

type Action struct {
	Method          Method                 `json:"method"`
	Protocol        Protocol               `json:"protocol"`
	Args            []string               `json:"args,omitempty"`
	ActiveIfNotRoot bool                   `json:"activeIfNotRoot"`
	IncludeValues   map[string]interface{} `json:"includeValues"`
	id              int
}

func (action *Action) SetId(id int) {
	action.id = id
}

func (action *Action) Id() int {
	return action.id
}
