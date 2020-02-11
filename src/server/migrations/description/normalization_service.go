package description

import (
	"server/errors"
	object2 "server/object"
	"utils"

	"server/migrations"
	"server/migrations/operations"
	"server/pg/migrations/operations/field"
	"server/pg/migrations/operations/object"
)

type NormalizationMigrationsFactory struct {
	metaDescriptionSyncer object2.MetaDescriptionSyncer
}

//Factories additional migration descriptions to handle links` changes
func (nmf *NormalizationMigrationsFactory) Factory(metaDescription *object2.Meta, operation operations.MigrationOperation) ([]*MigrationDescription, []*MigrationDescription, error) {
	runBefore := make([]*MigrationDescription, 0)
	runAfter := make([]*MigrationDescription, 0)
	switch concreteOperation := operation.(type) {
	case *object.CreateObjectOperation:
		if childRunAfter, err := nmf.factoryAddGenericOuterLinkMigrationsForMeta(concreteOperation.MetaDescription); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}

		if childRunAfter, err := nmf.factoryAddOuterLinkMigrationsForMeta(concreteOperation.MetaDescription); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}
		return runBefore, runAfter, nil
	case *field.AddFieldOperation:
		if childRunAfter, err := nmf.factoryAddGenericOuterLinkMigrationsForNewField(metaDescription, concreteOperation.Field); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}

		if childRunAfter, err := nmf.factoryAddOuterLinkMigrationsForNewField(metaDescription, concreteOperation.Field); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}

		runBefore = append(runBefore, nmf.factoryRemoveAutomaticallyAddedOuterField(metaDescription, concreteOperation.Field)...)
		return runBefore, runAfter, nil
	case *field.UpdateFieldOperation:
		return nmf.factoryAddGenericOuterLinkMigrationsForUpdatedField(metaDescription, concreteOperation.CurrentField, concreteOperation.NewField)
	case *object.RenameObjectOperation:
		if childRunAfter, err := nmf.factoryUpdateGenericOuterLinkMigrationsForMeta(metaDescription, concreteOperation.MetaDescription); err != nil {
			return runBefore, runAfter, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}
		if childRunAfter, err := nmf.factoryUpdateOuterLinkMigrationsForMeta(metaDescription, concreteOperation.MetaDescription); err != nil {
			return runBefore, runAfter, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}
		return runBefore, runAfter, nil
	case *object.DeleteObjectOperation:
		runBefore = append(runBefore, nmf.factoryRemoveGenericOuterLinkMigrationsForMeta(metaDescription)...)
		runBefore = append(runBefore, nmf.factoryRemoveOuterLinkMigrationsForMeta(metaDescription)...)
		return runBefore, nil, nil
	case *field.RemoveFieldOperation:
		runBefore = append(runBefore, nmf.factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription, concreteOperation.Field)...)
		runBefore = append(runBefore, nmf.factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription, concreteOperation.Field)...)
		return runBefore, nil, nil
	default:
		return nil, make([]*MigrationDescription, 0), nil
	}
}

//check meta for inner links and spawn a migrations to create an outer link
func (nmf *NormalizationMigrationsFactory) factoryAddOuterLinkMigrationsForMeta(metaDescription *object2.Meta) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for i := range metaDescription.Fields {
		if spawnedChildMigrations, err := nmf.factoryAddOuterLinkMigrationsForNewField(metaDescription, metaDescription.Fields[i]); err != nil {
			return spawnedMigrations, err
		} else {
			spawnedMigrations = append(spawnedMigrations, spawnedChildMigrations...)
		}
	}
	return spawnedMigrations, nil
}

//check meta for inner links and spawn a migrations to create an outer link
func (nmf *NormalizationMigrationsFactory) factoryAddOuterLinkMigrationsForNewField(metaName *object2.Meta, field *object2.Field, ) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	if field.Type == object2.FieldTypeObject && field.LinkType == object2.LinkTypeInner {
		linkMetaMap, _, err := nmf.metaDescriptionSyncer.Get(field.LinkMeta.Name)
		if err != nil {
			return nil, err
		}

		linkMetaDescription := object2.NewMetaFromMap(linkMetaMap)

		//add reverse outer
		fieldName := object2.ReverseInnerLinkName(metaName.Name)
		//automatically added outer field should only be available for querying
		outerField := object2.Field{
			Name:           fieldName,
			Type:           object2.FieldTypeArray,
			LinkType:       object2.LinkTypeOuter,
			LinkMeta:       metaName,
			OuterLinkField: field,
			Optional:       true,
			QueryMode:      true,
			RetrieveMode:   false,
		}

		addFieldOperationDescription := MigrationOperationDescription{
			Type:            AddFieldOperation,
			Field:           &MigrationFieldDescription{Field: outerField, PreviousName: ""},
			MetaDescription: linkMetaDescription,
		}

		spawnedMigrations = append(
			spawnedMigrations,
			&MigrationDescription{
				ApplyTo:    linkMetaDescription.Name,
				Operations: []MigrationOperationDescription{addFieldOperationDescription},
			},
		)
	}

	return spawnedMigrations, nil
}

//check meta for inner links and spawn a migrations to update references to meta and their names if they were generated automatically
func (nmf *NormalizationMigrationsFactory) factoryUpdateOuterLinkMigrationsForMeta(currentMetaDescription *object2.Meta, newMetaDescription *object2.Meta) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for _, field := range currentMetaDescription.Fields {
		if field.Type == object2.FieldTypeObject && field.LinkType == object2.LinkTypeInner {
			outerField := new(object2.MetaDescriptionManager).ReverseOuterField(currentMetaDescription.Name, field, nmf.metaDescriptionSyncer)

			updatedField := outerField.Clone()
			updatedField.LinkMeta = newMetaDescription

			if outerField.Name == object2.ReverseInnerLinkName(currentMetaDescription.Name) { //field is automatically generated
				updatedField.Name = object2.ReverseInnerLinkName(newMetaDescription.Name)
			}

			updateFieldOperationDescription := MigrationOperationDescription{
				Type:  UpdateFieldOperation,
				Field: &MigrationFieldDescription{Field: *updatedField, PreviousName: outerField.Name},
			}
			spawnedMigrations = append(spawnedMigrations, &MigrationDescription{
				ApplyTo:    field.LinkMeta.Name,
				Operations: []MigrationOperationDescription{updateFieldOperationDescription},
			})

		}
	}
	return spawnedMigrations, nil
}

//if an outer link field is explicitly specified and automatically created outer field exists
//this existing field should be removed first
func (nmf *NormalizationMigrationsFactory) factoryRemoveAutomaticallyAddedOuterField(metaDescription *object2.Meta, fieldToAdd *object2.Field) []*MigrationDescription {
	var existingOuterField *object2.Field
	if fieldToAdd.LinkType == object2.LinkTypeOuter {
		for i, field := range metaDescription.Fields {
			if field.OuterLinkField == fieldToAdd.OuterLinkField && field.LinkMeta == fieldToAdd.LinkMeta {
				if field.Type == fieldToAdd.Type {
					existingOuterField = metaDescription.Fields[i]
				}
			}
		}
	}
	if existingOuterField != nil {
		removeFieldOperationDescription := MigrationOperationDescription{
			Type:  RemoveFieldOperation,
			Field: &MigrationFieldDescription{Field: *existingOuterField, PreviousName: ""},
		}
		return []*MigrationDescription{
			{
				ApplyTo:    metaDescription.Name,
				Operations: []MigrationOperationDescription{removeFieldOperationDescription},
			},
		}
	}
	return []*MigrationDescription{}
}

func (nmf *NormalizationMigrationsFactory) factoryRemoveOuterLinkMigrationsForMeta(metaDescription *object2.Meta) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		spawnedMigrationDescriptions = append(spawnedMigrationDescriptions, nmf.factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription, field)...)
	}
	return spawnedMigrationDescriptions
}

func (nmf *NormalizationMigrationsFactory) factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription *object2.Meta, field *object2.Field) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	if field.Type == object2.FieldTypeObject && field.LinkType == object2.LinkTypeInner {
		outerField := new(object2.MetaDescriptionManager).ReverseOuterField(metaDescription.Name, field, nmf.metaDescriptionSyncer)
		removeFieldOperationDescription := MigrationOperationDescription{
			Type:  RemoveFieldOperation,
			Field: &MigrationFieldDescription{Field: *outerField, PreviousName: ""},
		}
		spawnedMigrationDescriptions = append(
			spawnedMigrationDescriptions,
			&MigrationDescription{
				ApplyTo:    field.LinkMeta.Name,
				Operations: []MigrationOperationDescription{removeFieldOperationDescription},
			},
		)
	}
	return spawnedMigrationDescriptions
}

//check meta for generic inner links and spawn a migrations to create an outer link
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForMeta(metaDescription *object2.Meta) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		if migrations, err := nmf.factoryAddGenericOuterLinkMigrationsForNewField(metaDescription, field); err != nil {
			return nil, err
		} else {
			spawnedMigrations = append(spawnedMigrations, migrations...)
		}
	}
	return spawnedMigrations, nil
}

//check if field is a generic inner link and spawn a migration to create an outer link
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForNewField(metaName *object2.Meta, field *object2.Field) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	if field.Type == object2.FieldTypeGeneric && field.LinkType == object2.LinkTypeInner {
		//add reverse outer link for each referenced meta
		for _, linkMetaName := range field.LinkMetaList {
			fieldName := object2.ReverseInnerLinkName(metaName.Name)
			linkMetaMap, _, err := nmf.metaDescriptionSyncer.Get(linkMetaName.Name)
			if err != nil {
				return nil, err
			}

			linkMetaDescription := object2.NewMetaFromMap(linkMetaMap)

			if linkMetaDescription.FindField(fieldName) != nil {
				continue
			}

			outerField := object2.Field{
				Name:           fieldName,
				Type:           object2.FieldTypeGeneric,
				LinkType:       object2.LinkTypeOuter,
				LinkMeta:       metaName,
				OuterLinkField: field,
				Optional:       true,
				QueryMode:      true,
				RetrieveMode:   false,
			}
			addFieldOperationDescription := MigrationOperationDescription{
				Type:            AddFieldOperation,
				Field:           &MigrationFieldDescription{Field: outerField, PreviousName: ""},
				MetaDescription: linkMetaDescription,
			}

			spawnedMigrations = append(
				spawnedMigrations,
				&MigrationDescription{
					ApplyTo:    linkMetaDescription.Name,
					Operations: []MigrationOperationDescription{addFieldOperationDescription},
				},
			)
		}
	}
	return spawnedMigrations, nil
}

//check meta for generic inner links and spawn a migrations to update references to meta and their names if they were generated automatically
func (nmf *NormalizationMigrationsFactory) factoryUpdateGenericOuterLinkMigrationsForMeta(currentMetaDescription *object2.Meta, newMetaDescription *object2.Meta) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for _, field := range currentMetaDescription.Fields {
		if field.Type == object2.FieldTypeGeneric && field.LinkType == object2.LinkTypeInner {
			for linkMetaName, outerField := range new(object2.MetaDescriptionManager).ReverseGenericOuterFields(currentMetaDescription.Name, field, nmf.metaDescriptionSyncer) {

				updatedField := outerField.Clone()
				updatedField.LinkMeta = newMetaDescription

				if outerField.Name == object2.ReverseInnerLinkName(currentMetaDescription.Name) { //field is automatically generated
					updatedField.Name = object2.ReverseInnerLinkName(newMetaDescription.Name)
				}

				updateFieldOperationDescription := MigrationOperationDescription{
					Type:  UpdateFieldOperation,
					Field: &MigrationFieldDescription{Field: *updatedField, PreviousName: outerField.Name},
				}
				spawnedMigrations = append(spawnedMigrations, &MigrationDescription{
					ApplyTo:    linkMetaName,
					Operations: []MigrationOperationDescription{updateFieldOperationDescription},
				})
			}
		}
	}
	return spawnedMigrations, nil
}

//check if field is a generic inner link and spawn a migration to create or remove an outer link
//TODO: now it does not handle case of type`s change
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForUpdatedField(metaName *object2.Meta, currentField *object2.Field, newField *object2.Field) ([]*MigrationDescription, []*MigrationDescription, error) {
	runAfter := make([]*MigrationDescription, 0)
	runBefore := make([]*MigrationDescription, 0)

	if currentField.Type == object2.FieldTypeGeneric && currentField.LinkType == object2.LinkTypeInner {
		if newField.Type == object2.FieldTypeGeneric && newField.LinkType == object2.LinkTypeInner {
			//if field has already been added

			excludedMetaNames := utils.Diff(currentField.GetLinkMetaListNames(), newField.GetLinkMetaListNames())
			includedMetaNames := utils.Diff(newField.GetLinkMetaListNames(), currentField.GetLinkMetaListNames())

			for _, excludedMetaName := range excludedMetaNames {
				outerField := object2.Field{
					Name:           object2.ReverseInnerLinkName(metaName.Name),
					Type:           object2.FieldTypeGeneric,
					LinkType:       object2.LinkTypeOuter,
					LinkMeta:       metaName,
					OuterLinkField: newField,
					Optional:       true,
					QueryMode:      true,
					RetrieveMode:   false,
				}

				removeFieldOperationDescription := MigrationOperationDescription{
					Type:  RemoveFieldOperation,
					Field: &MigrationFieldDescription{Field: outerField, PreviousName: ""},
				}
				runBefore = append(
					runBefore,
					&MigrationDescription{
						ApplyTo:    excludedMetaName,
						Operations: []MigrationOperationDescription{removeFieldOperationDescription},
					},
				)
			}

			for _, includedMetaName := range includedMetaNames {
				outerField := object2.Field{
					Name:           object2.ReverseInnerLinkName(metaName.Name),
					Type:           object2.FieldTypeGeneric,
					LinkType:       object2.LinkTypeOuter,
					LinkMeta:       metaName,
					OuterLinkField: newField,
					Optional:       true,
					QueryMode:      true,
					RetrieveMode:   false,
				}

				addFieldOperationDescription := MigrationOperationDescription{
					Type:  AddFieldOperation,
					Field: &MigrationFieldDescription{Field: outerField, PreviousName: ""},
				}
				runAfter = append(
					runAfter,
					&MigrationDescription{
						ApplyTo:    includedMetaName,
						Operations: []MigrationOperationDescription{addFieldOperationDescription},
					},
				)
			}
		} else {
			return nil, nil, errors.NewValidationError(migrations.MigrationErrorNotImplemented, "Generic inner link`s type change is not supported yet", nil)
		}
	}

	return runBefore, runAfter, nil
}

func (nmf *NormalizationMigrationsFactory) factoryRemoveGenericOuterLinkMigrationsForMeta(metaDescription *object2.Meta) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		spawnedMigrationDescriptions = append(spawnedMigrationDescriptions, nmf.factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription, field)...)
	}
	return spawnedMigrationDescriptions
}

func (nmf *NormalizationMigrationsFactory) factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription *object2.Meta, field *object2.Field) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	if field.Type == object2.FieldTypeGeneric && field.LinkType == object2.LinkTypeInner {
		for linkMetaName, outerFieldDescription := range new(object2.MetaDescriptionManager).ReverseGenericOuterFields(metaDescription.Name, field, nmf.metaDescriptionSyncer) {
			removeFieldOperationDescription := MigrationOperationDescription{
				Type:  RemoveFieldOperation,
				Field: &MigrationFieldDescription{Field: *outerFieldDescription, PreviousName: ""},
			}
			spawnedMigrationDescriptions = append(
				spawnedMigrationDescriptions,
				&MigrationDescription{
					ApplyTo:    linkMetaName,
					Operations: []MigrationOperationDescription{removeFieldOperationDescription},
				},
			)
		}
	}
	return spawnedMigrationDescriptions
}

func NewNormalizationMigrationsFactory(syncer object2.MetaDescriptionSyncer) *NormalizationMigrationsFactory {
	return &NormalizationMigrationsFactory{metaDescriptionSyncer: syncer}
}
