package meta

//Deprecated:
type MetaDescriptionManager struct {
}

func (mdm *MetaDescriptionManager) ReverseGenericOuterFields(ownerMetaName string, field *Field, syncer MetaDescriptionSyncer) map[string]*Field {
	if field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner {
		fieldMap := make(map[string]*Field, 0)
		for _, linkMetaName := range field.LinkMetaList {
			linkMetaMap, _, err := syncer.Get(linkMetaName.Name)
			if err != nil {
				panic(err.Error())
			}

			linkMetaDescription := NewMetaFromMap(linkMetaMap)

			for _, outerField := range linkMetaDescription.Fields {
				if outerField.Type == FieldTypeGeneric && outerField.LinkType == LinkTypeOuter {
					if outerField.OuterLinkField.Name == field.Name && outerField.LinkMeta.Name == ownerMetaName {
						fieldMap[linkMetaName.Name] = outerField
					}
				}
			}
		}
		return fieldMap
	}
	return nil
}

func (mdm *MetaDescriptionManager) ReverseOuterField(ownerMetaName string, field *Field, syncer MetaDescriptionSyncer) *Field {
	if field.Type == FieldTypeObject && field.LinkType == LinkTypeInner {
		linkMetaMap, _, err := syncer.Get(field.LinkMeta.Name)
		if err != nil {
			panic(err.Error())
		}

		linkMetaDescription := NewMetaFromMap(linkMetaMap)
		for _, outerField := range linkMetaDescription.Fields {
			if outerField.Type == FieldTypeArray && outerField.LinkType == LinkTypeOuter {
				if outerField.OuterLinkField.Name == field.Name && outerField.LinkMeta.Name == ownerMetaName {
					return outerField
				}
			}
		}
	}
	return nil
}
