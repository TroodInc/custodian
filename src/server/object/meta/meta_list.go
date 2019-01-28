package meta

type MetaList struct {
	metas []*Meta
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) GetLinkMetaListKeyFields() []*FieldDescription {
	fieldLinkMetaList := make([]*FieldDescription, 0)
	for _, meta := range metaList.metas {
		fieldLinkMetaList = append(fieldLinkMetaList, meta.Key)
	}
	return fieldLinkMetaList
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) AddMeta(meta *Meta) {
	metaList.metas = append(metaList.metas, meta)
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) GetAll() []*Meta {
	return metaList.metas
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) GetByName(metaName string) *Meta {
	for _, meta := range metaList.metas {
		if meta.Name == metaName {
			return meta
		}
	}
	return nil
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) Remove(meta *Meta) {
	indexOfTargetMeta := metaList.indexOf(meta)
	metaList.metas = append(metaList.metas[:indexOfTargetMeta], metaList.metas[indexOfTargetMeta+1:]...)
}

//returns list of key fields of LinkMetaList
func (metaList *MetaList) RemoveByName(metaName string) {
	meta := metaList.GetByName(metaName)
	indexOfTargetMeta := metaList.indexOf(meta)
	metaList.metas = append(metaList.metas[:indexOfTargetMeta], metaList.metas[indexOfTargetMeta+1:]...)
}

//returns A-side diff between two lists of meta
func (metaList *MetaList) Diff(metas []*Meta) []*Meta {
	diff := make([]*Meta, 0)
	for _, aMeta := range metaList.metas {
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

func (metaList *MetaList) indexOf(targetMeta *Meta) int {
	for i, meta := range metaList.metas {
		if meta.Name == targetMeta.Name {
			return i
		}
	}
	return -1
}
