package description_manager

import (
	"server/object/meta"
	"server/object/description"
)

type MetaDescriptionManager struct {
}

func (mdm *MetaDescriptionManager) ReverseGenericOuterFields(ownerMetaName string, field *description.Field, syncer meta.MetaDescriptionSyncer) map[string]*description.Field {
	if field.Type == description.FieldTypeGeneric && field.LinkType == description.LinkTypeInner {
		fieldMap := make(map[string]*description.Field, 0)
		for _, linkMetaName := range field.LinkMetaList {
			linkMetaDescription, _, err := syncer.Get(linkMetaName)
			if err != nil {
				panic(err.Error())
			}
			for _, outerField := range linkMetaDescription.Fields {
				if outerField.Type == description.FieldTypeGeneric && outerField.LinkType == description.LinkTypeOuter {
					if outerField.OuterLinkField == field.Name && outerField.LinkMeta == ownerMetaName {
						fieldMap[linkMetaName] = &outerField
					}
				}
			}
		}
		return fieldMap
	}
	return nil
}
