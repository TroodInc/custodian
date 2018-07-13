package meta

func MetaListDiff(aList, bList []*Meta) []*Meta {
	diff := make([]*Meta, 0)
	for _, aMeta := range aList {
		metaNotFound := true
		for _, bMeta := range bList {
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
