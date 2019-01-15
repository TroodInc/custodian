package description

import "github.com/getlantern/deepcopy"

//The shadow struct of the MetaDescription struct.
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

func (md *MetaDescription) FindField(fieldName string) *Field {
	for i, field := range md.Fields {
		if field.Name == fieldName {
			return &md.Fields[i]
		}
	}
	return nil
}

func (md *MetaDescription) ForExport() MetaDescription {
	metaCopy := MetaDescription{}
	deepcopy.Copy(&metaCopy, *md)
	for i := len(metaCopy.Fields) - 1; i >= 0; i-- {
		if metaCopy.Fields[i].LinkType == LinkTypeOuter {
			// exclude field supporting only query mode
			if !metaCopy.Fields[i].RetrieveMode {
				metaCopy.Fields = append(metaCopy.Fields[:i], metaCopy.Fields[i+1:]...)
				continue
			}
			//false value are interpreted as zero value
			metaCopy.Fields[i].RetrieveMode = false
			metaCopy.Fields[i].QueryMode = false
		}
	}
	return metaCopy
}

func NewMetaDescription(name string, key string, fields []Field, actions []Action, cas bool) *MetaDescription {
	return &MetaDescription{Name: name, Key: key, Fields: fields, Actions: actions, Cas: cas}
}
