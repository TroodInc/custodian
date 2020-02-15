package meta


//TODO: Looks redundant, may be moved to Meta.AddField method
//Deprecated:
type NormalizationService struct {
}

//Set not specified default values
//Deprecated:
func (normalizationService *NormalizationService) Normalize(metaDescription *Meta) *Meta {
	normalizationService.NormalizeInnerFields(metaDescription.Fields)
	normalizationService.NormalizeOuterFields(metaDescription.Fields)
	return metaDescription
}

//set default ON_DELETE strategy value
func (normalizationService *NormalizationService) NormalizeInnerFields(fields map[string]*Field) {
	for i, field := range fields {
		if field.Type == FieldTypeObject ||
			(field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner) {
			if field.OnDelete == "" {
				fields[i].OnDelete = OnDeleteCascadeVerbose
			}
		}
	}
}

//set correct modes for outer field
func (normalizationService *NormalizationService) NormalizeOuterFields(fields map[string]*Field) {
	for i, field := range fields {
		if field.LinkType == LinkTypeOuter {
			fields[i].QueryMode = true
			fields[i].RetrieveMode = true
		}
	}
}
