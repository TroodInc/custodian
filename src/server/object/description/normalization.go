package description

type NormalizationService struct {
}

//Set not specified default values
func (normalizationService *NormalizationService) Normalize(metaDescription *MetaDescription) *MetaDescription {
	normalizationService.NormalizeInnerFields(&metaDescription.Fields)
	normalizationService.NormalizeOuterFields(&metaDescription.Fields)
	return metaDescription
}

//set default ON_DELETE strategy value
func (normalizationService *NormalizationService) NormalizeInnerFields(fields *[]Field) {
	for i, field := range *fields {
		if field.Type == FieldTypeObject ||
			(field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner) {
			if field.OnDelete == "" {
				(*fields)[i].OnDelete = OnDeleteCascadeVerbose
			}
		}
	}
}

//set correct modes for outer field
func (normalizationService *NormalizationService) NormalizeOuterFields(fields *[]Field) {
	for i, field := range *fields {
		if field.LinkType == LinkTypeOuter {
			(*fields)[i].QueryMode = true
			(*fields)[i].RetrieveMode = true
		}
	}
}
