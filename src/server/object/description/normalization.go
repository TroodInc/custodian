package description

type NormalizationService struct {
}

//Set not specified default values
func (normalizationService *NormalizationService) Normalize(metaDescription *MetaDescription) *MetaDescription {
	normalizationService.NormalizeInnerField(&metaDescription.Fields)
	return metaDescription
}

//set default ON_DELETE strategy value
func (normalizationService *NormalizationService) NormalizeInnerField(fields *[]Field) {
	for i, field := range *fields {
		if field.Type == FieldTypeObject ||
			(field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner) {
			if field.OnDelete == "" {
				(*fields)[i].OnDelete = OnDeleteCascadeVerbose
			}
		}
	}
}
