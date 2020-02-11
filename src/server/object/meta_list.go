package object

type MetaList struct {
	Metas []*Meta
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) AddMeta(meta *Meta) {
	metaList.Metas = append(metaList.Metas, meta)
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) GetAll() []*Meta {
	return metaList.Metas
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) GetByName(metaName string) *Meta {
	for _, meta := range metaList.Metas {
		if meta.Name == metaName {
			return meta
		}
	}
	return nil
}

//returns A-side diff between two lists of meta
func (metaList *MetaList) Diff(metas []*Meta) []*Meta {
	diff := make([]*Meta, 0)
	for _, aMeta := range metaList.Metas {
		metaNotFound := true
		for _, bMeta := range metas {
			if bMeta.Name == aMeta.Name {
				metaNotFound = false
			}
		}
		if metaNotFound {
			diff = append(diff, aMeta)
		}
	}
	return diff
}