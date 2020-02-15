package description

import (
	"server/errors"
	"server/object/meta"
	"utils"
	"server/migrations"
	"server/migrations/operations"
	"server/pg/migrations/operations/field"
	"server/pg/migrations/operations/object"
)

type NormalizationMigrationsFactory struct {
	metaStore meta.MetaDescriptionSyncer
}

//Factories additional migration descriptions to handle links` changes
func (nmf *NormalizationMigrationsFactory) Factory(metaDescription *meta.Meta, operation operations.MigrationOperation) ([]*MigrationDescription, []*MigrationDescription, error) {
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
func (nmf *NormalizationMigrationsFactory) factoryAddOuterLinkMigrationsForMeta(metaDescription *meta.Meta) ([]*MigrationDescription, error) {
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
func (nmf *NormalizationMigrationsFactory) factoryAddOuterLinkMigrationsForNewField(metaName *meta.Meta, field *meta.Field, ) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	if field.Type == meta.FieldTypeObject && field.LinkType == meta.LinkTypeInner {
		linkMetaMap, _, err := nmf.metaStore.Get(field.LinkMeta.Name)
		if err != nil {
			return nil, err
		}

		linkMetaDescription := meta.NewMetaFromMap(linkMetaMap)

		//add reverse outer
		fieldName := meta.ReverseInnerLinkName(metaName.Name)
		//automatically added outer field should only be available for querying
		outerField := meta.Field{
			Name:           fieldName,
			Type:           meta.FieldTypeArray,
			LinkType:       meta.LinkTypeOuter,
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
func (nmf *NormalizationMigrationsFactory) factoryUpdateOuterLinkMigrationsForMeta(currentMetaDescription *meta.Meta, newMetaDescription *meta.Meta) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for _, field := range currentMetaDescription.Fields {
		if field.Type == meta.FieldTypeObject && field.LinkType == meta.LinkTypeInner {
			outerField := new(meta.MetaDescriptionManager).ReverseOuterField(currentMetaDescription.Name, field, nmf.metaStore)

			updatedField := outerField.Clone()
			updatedField.LinkMeta = newMetaDescription

			if outerField.Name == meta.ReverseInnerLinkName(currentMetaDescription.Name) { //field is automatically generated
				updatedField.Name = meta.ReverseInnerLinkName(newMetaDescription.Name)
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
func (nmf *NormalizationMigrationsFactory) factoryRemoveAutomaticallyAddedOuterField(metaDescription *meta.Meta, fieldToAdd *meta.Field) []*MigrationDescription {
	var existingOuterField *meta.Field
	if fieldToAdd.LinkType == meta.LinkTypeOuter {
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

func (nmf *NormalizationMigrationsFactory) factoryRemoveOuterLinkMigrationsForMeta(metaDescription *meta.Meta) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		spawnedMigrationDescriptions = append(spawnedMigrationDescriptions, nmf.factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription, field)...)
	}
	return spawnedMigrationDescriptions
}

func (nmf *NormalizationMigrationsFactory) factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription *meta.Meta, field *meta.Field) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	if field.Type == meta.FieldTypeObject && field.LinkType == meta.LinkTypeInner {
		outerField := new(meta.MetaDescriptionManager).ReverseOuterField(metaDescription.Name, field, nmf.metaStore)
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
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForMeta(metaDescription *meta.Meta) ([]*MigrationDescription, error) {
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
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForNewField(metaName *meta.Meta, field *meta.Field) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	if field.Type == meta.FieldTypeGeneric && field.LinkType == meta.LinkTypeInner {
		//add reverse outer link for each referenced meta
		for _, linkMetaName := range field.LinkMetaList {
			fieldName := meta.ReverseInnerLinkName(metaName.Name)
			linkMetaMap, _, err := nmf.metaStore.Get(linkMetaName.Name)
			if err != nil {
				return nil, err
			}

			linkMetaDescription := meta.NewMetaFromMap(linkMetaMap)

			if linkMetaDescription.FindField(fieldName) != nil {
				continue
			}

			outerField := meta.Field{
				Name:           fieldName,
				Type:           meta.FieldTypeGeneric,
				LinkType:       meta.LinkTypeOuter,
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
func (nmf *NormalizationMigrationsFactory) factoryUpdateGenericOuterLinkMigrationsForMeta(currentMetaDescription *meta.Meta, newMetaDescription *meta.Meta) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for _, field := range currentMetaDescription.Fields {
		if field.Type == meta.FieldTypeGeneric && field.LinkType == meta.LinkTypeInner {
			for linkMetaName, outerField := range new(meta.MetaDescriptionManager).ReverseGenericOuterFields(currentMetaDescription.Name, field, nmf.metaStore) {

				updatedField := outerField.Clone()
				updatedField.LinkMeta = newMetaDescription

				if outerField.Name == meta.ReverseInnerLinkName(currentMetaDescription.Name) { //field is automatically generated
					updatedField.Name = meta.ReverseInnerLinkName(newMetaDescription.Name)
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
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForUpdatedField(metaName *meta.Meta, currentField *meta.Field, newField *meta.Field) ([]*MigrationDescription, []*MigrationDescription, error) {
	runAfter := make([]*MigrationDescription, 0)
	runBefore := make([]*MigrationDescription, 0)

	if currentField.Type == meta.FieldTypeGeneric && currentField.LinkType == meta.LinkTypeInner {
		if newField.Type == meta.FieldTypeGeneric && newField.LinkType == meta.LinkTypeInner {
			//if field has already been added

			excludedMetaNames := utils.Diff(currentField.GetLinkMetaListNames(), newField.GetLinkMetaListNames())
			includedMetaNames := utils.Diff(newField.GetLinkMetaListNames(), currentField.GetLinkMetaListNames())

			for _, excludedMetaName := range excludedMetaNames {
				outerField := meta.Field{
					Name:           meta.ReverseInnerLinkName(metaName.Name),
					Type:           meta.FieldTypeGeneric,
					LinkType:       meta.LinkTypeOuter,
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
				outerField := meta.Field{
					Name:           meta.ReverseInnerLinkName(metaName.Name),
					Type:           meta.FieldTypeGeneric,
					LinkType:       meta.LinkTypeOuter,
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

func (nmf *NormalizationMigrationsFactory) factoryRemoveGenericOuterLinkMigrationsForMeta(metaDescription *meta.Meta) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		spawnedMigrationDescriptions = append(spawnedMigrationDescriptions, nmf.factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription, field)...)
	}
	return spawnedMigrationDescriptions
}

func (nmf *NormalizationMigrationsFactory) factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription *meta.Meta, field *meta.Field) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	if field.Type == meta.FieldTypeGeneric && field.LinkType == meta.LinkTypeInner {
		for linkMetaName, outerFieldDescription := range new(meta.MetaDescriptionManager).ReverseGenericOuterFields(metaDescription.Name, field, nmf.metaStore) {
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

func NewNormalizationMigrationsFactory(syncer meta.MetaDescriptionSyncer) *NormalizationMigrationsFactory {
	return &NormalizationMigrationsFactory{metaStore: syncer}
}
