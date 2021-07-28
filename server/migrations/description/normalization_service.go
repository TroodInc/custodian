package description

import (
	"custodian/server/errors"
	"custodian/server/migrations"
	"custodian/server/migrations/operations"
	object2 "custodian/server/object"
	"custodian/server/object/description"
	object_description "custodian/server/object/description"
	"custodian/server/object/description_manager"
	"custodian/server/object/migrations/operations/field"
	"custodian/server/object/migrations/operations/object"
	"fmt"
)

type NormalizationMigrationsFactory struct {
	metaDescriptionSyncer object2.MetaDescriptionSyncer
}

//Factories additional migration descriptions to handle links` changes
func (nmf *NormalizationMigrationsFactory) Factory(metaDescription *object_description.MetaDescription, operation operations.MigrationOperation) ([]*MigrationDescription, []*MigrationDescription, error) {
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

		if childRunAfter, err := nmf.factoryAddObjectsFieldOnObjectCreate(concreteOperation.MetaDescription); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}
		return runBefore, runAfter, nil
	case *field.AddFieldOperation:
		if childRunAfter, err := nmf.factoryAddGenericOuterLinkMigrationsForNewField(metaDescription.Name, concreteOperation.Field); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}

		if childRunAfter, err := nmf.factoryAddOuterLinkMigrationsForNewField(metaDescription.Name, concreteOperation.Field); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}
		if childRunAfter, err := nmf.factoryAddObjectsLinkMigrationsForNewField(metaDescription.Name, concreteOperation.Field); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}

		runBefore = append(runBefore, nmf.factoryRemoveAutomaticallyAddedOuterField(metaDescription, concreteOperation.Field)...)
		return runBefore, runAfter, nil
	case *field.UpdateFieldOperation:
		return nmf.factoryAddGenericOuterLinkMigrationsForUpdatedField(metaDescription.Name, concreteOperation.CurrentField, concreteOperation.NewField)
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
func (nmf *NormalizationMigrationsFactory) factoryAddOuterLinkMigrationsForMeta(metaDescription *object_description.MetaDescription) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for i := range metaDescription.Fields {
		if spawnedChildMigrations, err := nmf.factoryAddOuterLinkMigrationsForNewField(metaDescription.Name, &metaDescription.Fields[i]); err != nil {
			return spawnedMigrations, err
		} else {
			spawnedMigrations = append(spawnedMigrations, spawnedChildMigrations...)
		}
	}
	return spawnedMigrations, nil
}

func (nmf *NormalizationMigrationsFactory) factoryAddObjectsFieldOnObjectCreate(metaDescription *object_description.MetaDescription) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for i := range metaDescription.Fields {
		if metaDescription.Fields[i].Type == object_description.FieldTypeObjects {
			throughMetaName := fmt.Sprintf("%s__%s", metaDescription.Name, metaDescription.Fields[i].LinkMeta)

			if childRunAfter, err := nmf.factoryAddOuterLinkMigrationsForNewField(throughMetaName, &metaDescription.Fields[i]); err != nil {
				return nil, err
			} else {
				spawnedMigrations = append(spawnedMigrations, childRunAfter...)
			}

			if childRunAfter, err := nmf.factoryAddOuterLinkMigrationsForNewFieldIfNotCreatedYet(throughMetaName, &metaDescription.Fields[i], metaDescription); err != nil {
				return nil, err
			} else {
				spawnedMigrations = append(spawnedMigrations, childRunAfter...)
			}

			if childRunAfter, err := CreateThroughMetaMigration(metaDescription.Name, &metaDescription.Fields[i]); err != nil {
				return nil, err
			} else {
				spawnedMigrations = append(spawnedMigrations, childRunAfter...)
			}

		}
	}

	return spawnedMigrations, nil
}

//check meta for inner links and spawn a migrations to create an outer link
func (nmf *NormalizationMigrationsFactory) factoryAddOuterLinkMigrationsForNewField(metaName string, field *object_description.Field) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	if field.Type == object_description.FieldTypeObject && field.LinkType == object_description.LinkTypeInner {
		linkMetaDescription, _, err := nmf.metaDescriptionSyncer.Get(field.LinkMeta)
		if err != nil {
			return nil, err
		}

		//add reverse outer
		fieldName := object2.ReverseInnerLinkName(metaName)
		//automatically added outer field should only be available for querying
		outerField := object_description.Field{
			Name:           fieldName,
			Type:           object_description.FieldTypeArray,
			LinkType:       object_description.LinkTypeOuter,
			LinkMeta:       metaName,
			OuterLinkField: field.Name,
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

func (nmf *NormalizationMigrationsFactory) factoryAddOuterLinkMigrationsForNewFieldIfNotCreatedYet(metaName string, field *object_description.Field, metaDescription *object_description.MetaDescription) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	if field.Type == object_description.FieldTypeObject && field.LinkType == object_description.LinkTypeInner {

		//add reverse outer
		fieldName := object2.ReverseInnerLinkName(metaName)
		//automatically added outer field should only be available for querying
		outerField := object_description.Field{
			Name:           fieldName,
			Type:           object_description.FieldTypeArray,
			LinkType:       object_description.LinkTypeOuter,
			LinkMeta:       metaName,
			OuterLinkField: field.Name,
			Optional:       true,
			QueryMode:      true,
			RetrieveMode:   false,
		}

		addFieldOperationDescription := MigrationOperationDescription{
			Type:            AddFieldOperation,
			Field:           &MigrationFieldDescription{Field: outerField, PreviousName: ""},
			MetaDescription: metaDescription,
		}

		spawnedMigrations = append(
			spawnedMigrations,
			&MigrationDescription{
				ApplyTo:    metaDescription.Name,
				Operations: []MigrationOperationDescription{addFieldOperationDescription},
			},
		)
	}

	return spawnedMigrations, nil
}

func (nmf *NormalizationMigrationsFactory) factoryAddObjectsLinkMigrationsForNewField(metaName string, field *object_description.Field) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	if field.Type == object_description.FieldTypeObjects {
		throughMetaName := fmt.Sprintf("%s__%s", metaName, field.LinkMeta)

		secondM2Mfield := description.Field{
			Name:     metaName,
			Type:     description.FieldTypeObject,
			LinkMeta: metaName,
			LinkType: description.LinkTypeInner,
			Optional: false,
		}
		for _, f := range []*object_description.Field{field, &secondM2Mfield} {
			if childRunAfter, err := nmf.factoryAddOuterLinkMigrationsForNewField(throughMetaName, f); err != nil {
				return nil, err
			} else {
				spawnedMigrations = append(spawnedMigrations, childRunAfter...)
			}
		}
		if childRunAfter, err := CreateThroughMetaMigration(metaName, field); err != nil {
			return nil, err
		} else {
			spawnedMigrations = append(spawnedMigrations, childRunAfter...)
		}

	}

	return spawnedMigrations, nil
}

func CreateThroughMetaMigration(metaName string, field *object_description.Field) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)

	throughMetaName := fmt.Sprintf("%s__%s", metaName, field.LinkMeta)

	fields := []description.Field{
		{
			Name: "id",
			Type: description.FieldTypeNumber,
			Def: map[string]interface{}{
				"func": "nextval",
			},
			Optional: true,
		},
		{
			Name:     metaName,
			Type:     description.FieldTypeObject,
			LinkMeta: metaName,
			LinkType: description.LinkTypeInner,
			Optional: false,
		},
		{
			Name:     field.LinkMeta,
			Type:     description.FieldTypeObject,
			LinkMeta: field.LinkMeta,
			LinkType: description.LinkTypeInner,
			Optional: false,
		},
	}
	throughMetaDescription := description.NewMetaDescription(throughMetaName, "id", fields, []description.Action{}, false)
	throughMigrationDescription := NewMigrationOperationDescription(CreateObjectOperation, nil, throughMetaDescription, nil)
	spawnedMigrations = append(
		spawnedMigrations,
		&MigrationDescription{
			ApplyTo:    "",
			Operations: []MigrationOperationDescription{*throughMigrationDescription},
		},
	)
	return spawnedMigrations, nil
}

//check meta for inner links and spawn a migrations to update references to meta and their names if they were generated automatically
func (nmf *NormalizationMigrationsFactory) factoryUpdateOuterLinkMigrationsForMeta(currentMetaDescription *object_description.MetaDescription, newMetaDescription *object_description.MetaDescription) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for _, field := range currentMetaDescription.Fields {
		if field.Type == object_description.FieldTypeObject && field.LinkType == object_description.LinkTypeInner {
			outerField := new(description_manager.MetaDescriptionManager).ReverseOuterField(currentMetaDescription.Name, &field, nmf.metaDescriptionSyncer)

			updatedField := outerField.Clone()
			updatedField.LinkMeta = newMetaDescription.Name

			if outerField.Name == object2.ReverseInnerLinkName(currentMetaDescription.Name) { //field is automatically generated
				updatedField.Name = object2.ReverseInnerLinkName(newMetaDescription.Name)
			}

			updateFieldOperationDescription := MigrationOperationDescription{
				Type:  UpdateFieldOperation,
				Field: &MigrationFieldDescription{Field: *updatedField, PreviousName: outerField.Name},
			}
			spawnedMigrations = append(spawnedMigrations, &MigrationDescription{
				ApplyTo:    field.LinkMeta,
				Operations: []MigrationOperationDescription{updateFieldOperationDescription},
			})

		}
	}
	return spawnedMigrations, nil
}

//if an outer link field is explicitly specified and automatically created outer field exists
//this existing field should be removed first
func (nmf *NormalizationMigrationsFactory) factoryRemoveAutomaticallyAddedOuterField(metaDescription *object_description.MetaDescription, fieldToAdd *object_description.Field) []*MigrationDescription {
	var existingOuterField *object_description.Field
	if fieldToAdd.LinkType == object_description.LinkTypeOuter {
		for i, field := range metaDescription.Fields {
			if field.OuterLinkField == fieldToAdd.OuterLinkField && field.LinkMeta == fieldToAdd.LinkMeta {
				if field.Type == fieldToAdd.Type {
					existingOuterField = &metaDescription.Fields[i]
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

func (nmf *NormalizationMigrationsFactory) factoryRemoveOuterLinkMigrationsForMeta(metaDescription *object_description.MetaDescription) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		spawnedMigrationDescriptions = append(spawnedMigrationDescriptions, nmf.factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription, &field)...)
	}
	return spawnedMigrationDescriptions
}

func (nmf *NormalizationMigrationsFactory) factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription *object_description.MetaDescription, field *object_description.Field) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	if field.Type == object_description.FieldTypeObject && field.LinkType == object_description.LinkTypeInner {
		outerField := new(description_manager.MetaDescriptionManager).ReverseOuterField(metaDescription.Name, field, nmf.metaDescriptionSyncer)
		removeFieldOperationDescription := MigrationOperationDescription{
			Type:  RemoveFieldOperation,
			Field: &MigrationFieldDescription{Field: *outerField, PreviousName: ""},
		}
		spawnedMigrationDescriptions = append(
			spawnedMigrationDescriptions,
			&MigrationDescription{
				ApplyTo:    field.LinkMeta,
				Operations: []MigrationOperationDescription{removeFieldOperationDescription},
			},
		)
	}
	return spawnedMigrationDescriptions
}

//check meta for generic inner links and spawn a migrations to create an outer link
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForMeta(metaDescription *object_description.MetaDescription) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		if migrations, err := nmf.factoryAddGenericOuterLinkMigrationsForNewField(metaDescription.Name, &field); err != nil {
			return nil, err
		} else {
			spawnedMigrations = append(spawnedMigrations, migrations...)
		}
	}
	return spawnedMigrations, nil
}

//check if field is a generic inner link and spawn a migration to create an outer link
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForNewField(metaName string, field *object_description.Field) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	if field.Type == object_description.FieldTypeGeneric && field.LinkType == object_description.LinkTypeInner {
		//add reverse outer link for each referenced meta
		for _, linkMetaName := range field.LinkMetaList {
			fieldName := object2.ReverseInnerLinkName(metaName)
			linkMetaDescription, _, err := nmf.metaDescriptionSyncer.Get(linkMetaName)
			if err != nil {
				return nil, err
			}
			if linkMetaDescription.FindField(fieldName) != nil {
				continue
			}

			outerField := object_description.Field{
				Name:           fieldName,
				Type:           object_description.FieldTypeGeneric,
				LinkType:       object_description.LinkTypeOuter,
				LinkMeta:       metaName,
				OuterLinkField: field.Name,
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
func (nmf *NormalizationMigrationsFactory) factoryUpdateGenericOuterLinkMigrationsForMeta(currentMetaDescription *object_description.MetaDescription, newMetaDescription *object_description.MetaDescription) ([]*MigrationDescription, error) {
	spawnedMigrations := make([]*MigrationDescription, 0)
	for _, field := range currentMetaDescription.Fields {
		if field.Type == object_description.FieldTypeGeneric && field.LinkType == object_description.LinkTypeInner {
			for linkMetaName, outerField := range new(description_manager.MetaDescriptionManager).ReverseGenericOuterFields(currentMetaDescription.Name, &field, nmf.metaDescriptionSyncer) {

				updatedField := outerField.Clone()
				updatedField.LinkMeta = newMetaDescription.Name

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
func (nmf *NormalizationMigrationsFactory) factoryAddGenericOuterLinkMigrationsForUpdatedField(metaName string, currentField *object_description.Field, newField *object_description.Field) ([]*MigrationDescription, []*MigrationDescription, error) {
	runAfter := make([]*MigrationDescription, 0)
	runBefore := make([]*MigrationDescription, 0)

	if currentField.Type == object_description.FieldTypeGeneric && currentField.LinkType == object_description.LinkTypeInner {
		if newField.Type == object_description.FieldTypeGeneric && newField.LinkType == object_description.LinkTypeInner {
			//if field has already been added

			excludedMetaNames := currentField.LinkMetaList.Diff(newField.LinkMetaList)
			includedMetaNames := newField.LinkMetaList.Diff(currentField.LinkMetaList)

			for _, excludedMetaName := range excludedMetaNames {
				outerField := object_description.Field{
					Name:           object2.ReverseInnerLinkName(metaName),
					Type:           object_description.FieldTypeGeneric,
					LinkType:       object_description.LinkTypeOuter,
					LinkMeta:       metaName,
					OuterLinkField: newField.Name,
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
				outerField := object_description.Field{
					Name:           object2.ReverseInnerLinkName(metaName),
					Type:           object_description.FieldTypeGeneric,
					LinkType:       object_description.LinkTypeOuter,
					LinkMeta:       metaName,
					OuterLinkField: newField.Name,
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

func (nmf *NormalizationMigrationsFactory) factoryRemoveGenericOuterLinkMigrationsForMeta(metaDescription *object_description.MetaDescription) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		spawnedMigrationDescriptions = append(spawnedMigrationDescriptions, nmf.factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription, &field)...)
	}
	return spawnedMigrationDescriptions
}

func (nmf *NormalizationMigrationsFactory) factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription *object_description.MetaDescription, field *object_description.Field) []*MigrationDescription {
	spawnedMigrationDescriptions := make([]*MigrationDescription, 0)
	if field.Type == object_description.FieldTypeGeneric && field.LinkType == object_description.LinkTypeInner {
		for linkMetaName, outerFieldDescription := range new(description_manager.MetaDescriptionManager).ReverseGenericOuterFields(metaDescription.Name, field, nmf.metaDescriptionSyncer) {
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
