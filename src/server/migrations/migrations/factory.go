package migrations

import (
	"server/object/meta"
	"server/transactions"
	pg_operations "server/pg/migrations/operations"
	object_description "server/object/description"
	"server/migrations/description"
	"server/migrations/operations"
	"server/pg/migrations/operations/object"
	"server/pg/migrations/operations/field"
	"server/migrations"
	"server/object/description_manager"
)

type MigrationFactory struct {
	metaStore             *meta.MetaStore
	metaDescriptionSyncer meta.MetaDescriptionSyncer
	globalTransaction     *transactions.GlobalTransaction
}

func (mf *MigrationFactory) Factory(migrationDescription *description.MigrationDescription) (*Migration, error) {
	migration := &Migration{MigrationDescription: *migrationDescription}

	if migration.MigrationDescription.ApplyTo != "" {
		if applyTo, _, err := mf.metaDescriptionSyncer.Get(migration.MigrationDescription.ApplyTo); err != nil {
			return nil, err
		} else {
			migration.ApplyTo = applyTo
		}
	}

	operationFactory := pg_operations.NewOperationFactory()

	for i := range migration.MigrationDescription.Operations {
		operation, err := operationFactory.Factory(&migration.MigrationDescription.Operations[i], migration.ApplyTo)
		if err != nil {
			return nil, err
		}

		migration.Operations = append(migration.Operations, operation)

		runBefore, runAfter, err := mf.factoryNormalizationMigrations(migration.ApplyTo, operation)
		if err != nil {
			return nil, err
		}
		if len(runBefore) > 0 {
			migration.RunBefore = append(migration.RunBefore, runBefore...)
		}
		if len(runAfter) > 0 {
			migration.RunAfter = append(migration.RunAfter, runAfter...)
		}
	}
	return migration, nil
}

func (mf *MigrationFactory) factoryNormalizationMigrations(metaDescription *object_description.MetaDescription, operation operations.MigrationOperation) ([]*description.MigrationDescription, []*description.MigrationDescription, error) {
	runBefore := make([]*description.MigrationDescription, 0)
	runAfter := make([]*description.MigrationDescription, 0)
	switch concreteOperation := operation.(type) {
	case *object.CreateObjectOperation:
		if childRunAfter, err := mf.factoryAddGenericOuterLinkMigrationsForMeta(concreteOperation.MetaDescription); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}

		if childRunAfter, err := mf.factoryAddOuterLinkMigrationsForMeta(concreteOperation.MetaDescription); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}
		return runBefore, runAfter, nil
	case *field.AddFieldOperation:
		if childRunAfter, err := mf.factoryAddGenericOuterLinkMigrationsForNewField(metaDescription.Name, concreteOperation.Field); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}

		if childRunAfter, err := mf.factoryAddOuterLinkMigrationsForNewField(metaDescription.Name, concreteOperation.Field); err != nil {
			return nil, nil, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}

		runBefore = append(runBefore, mf.factoryRemoveAutomaticallyAddedOuterField(metaDescription, concreteOperation.Field)...)
		return runBefore, runAfter, nil
	case *field.UpdateFieldOperation:
		return mf.factoryAddGenericOuterLinkMigrationsForUpdatedField(metaDescription.Name, concreteOperation.CurrentField, concreteOperation.NewField)
	case *object.RenameObjectOperation:
		if childRunAfter, err := mf.factoryUpdateGenericOuterLinkMigrationsForMeta(metaDescription, concreteOperation.MetaDescription); err != nil {
			return runBefore, runAfter, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}
		if childRunAfter, err := mf.factoryUpdateOuterLinkMigrationsForMeta(metaDescription, concreteOperation.MetaDescription); err != nil {
			return runBefore, runAfter, err
		} else {
			runAfter = append(runAfter, childRunAfter...)
		}
		return runBefore, runAfter, nil
	case *object.DeleteObjectOperation:
		runBefore = append(runBefore, mf.factoryRemoveGenericOuterLinkMigrationsForMeta(metaDescription)...)
		runBefore = append(runBefore, mf.factoryRemoveOuterLinkMigrationsForMeta(metaDescription)...)
		return runBefore, nil, nil
	case *field.RemoveFieldOperation:
		runBefore = append(runBefore, mf.factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription, concreteOperation.Field)...)
		runBefore = append(runBefore, mf.factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription, concreteOperation.Field)...)
		return runBefore, nil, nil
	default:
		return nil, make([]*description.MigrationDescription, 0), nil
	}
}

//check meta for inner links and spawn a migrations to create an outer link
func (mf *MigrationFactory) factoryAddOuterLinkMigrationsForMeta(metaDescription *object_description.MetaDescription) ([]*description.MigrationDescription, error) {
	spawnedMigrations := make([]*description.MigrationDescription, 0)
	for i := range metaDescription.Fields {
		if spawnedChildMigrations, err := mf.factoryAddOuterLinkMigrationsForNewField(metaDescription.Name, &metaDescription.Fields[i]); err != nil {
			return spawnedMigrations, err
		} else {
			spawnedMigrations = append(spawnedMigrations, spawnedChildMigrations...)
		}
	}
	return spawnedMigrations, nil
}

//check meta for inner links and spawn a migrations to create an outer link
func (mf *MigrationFactory) factoryAddOuterLinkMigrationsForNewField(metaName string, field *object_description.Field, ) ([]*description.MigrationDescription, error) {
	spawnedMigrations := make([]*description.MigrationDescription, 0)
	if field.Type == object_description.FieldTypeObject && field.LinkType == object_description.LinkTypeInner {
		linkMetaDescription, _, err := mf.metaDescriptionSyncer.Get(field.LinkMeta)
		if err != nil {
			return nil, err
		}

		//add reverse outer
		fieldName := meta.ReverseInnerLinkName(metaName)
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

		addFieldOperationDescription := description.MigrationOperationDescription{
			Type:            description.AddFieldOperation,
			Field:           &description.MigrationFieldDescription{Field: outerField, PreviousName: ""},
			MetaDescription: linkMetaDescription,
		}

		spawnedMigrations = append(
			spawnedMigrations,
			&description.MigrationDescription{
				ApplyTo:    linkMetaDescription.Name,
				Operations: []description.MigrationOperationDescription{addFieldOperationDescription},
			},
		)
	}

	return spawnedMigrations, nil
}

//check meta for inner links and spawn a migrations to update references to meta and their names if they were generated automatically
func (mf *MigrationFactory) factoryUpdateOuterLinkMigrationsForMeta(currentMetaDescription *object_description.MetaDescription, newMetaDescription *object_description.MetaDescription) ([]*description.MigrationDescription, error) {
	spawnedMigrations := make([]*description.MigrationDescription, 0)
	for _, field := range currentMetaDescription.Fields {
		if field.Type == object_description.FieldTypeObject && field.LinkType == object_description.LinkTypeInner {
			outerField := new(description_manager.MetaDescriptionManager).ReverseOuterField(currentMetaDescription.Name, &field, mf.metaDescriptionSyncer)

			updatedField := outerField.Clone()
			updatedField.LinkMeta = newMetaDescription.Name

			if outerField.Name == meta.ReverseInnerLinkName(currentMetaDescription.Name) { //field is automatically generated
				updatedField.Name = meta.ReverseInnerLinkName(newMetaDescription.Name)
			}

			updateFieldOperationDescription := description.MigrationOperationDescription{
				Type:  description.UpdateFieldOperation,
				Field: &description.MigrationFieldDescription{Field: *updatedField, PreviousName: outerField.Name},
			}
			spawnedMigrations = append(spawnedMigrations, &description.MigrationDescription{
				ApplyTo:    field.LinkMeta,
				Operations: []description.MigrationOperationDescription{updateFieldOperationDescription},
			})

		}
	}
	return spawnedMigrations, nil
}

//if an outer link field is explicitly specified and automatically created outer field exists
//this existing field should be removed first
func (mf *MigrationFactory) factoryRemoveAutomaticallyAddedOuterField(metaDescription *object_description.MetaDescription, fieldToAdd *object_description.Field) []*description.MigrationDescription {
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
		removeFieldOperationDescription := description.MigrationOperationDescription{
			Type:  description.RemoveFieldOperation,
			Field: &description.MigrationFieldDescription{Field: *existingOuterField, PreviousName: ""},
		}
		return []*description.MigrationDescription{
			{
				ApplyTo:    metaDescription.Name,
				Operations: []description.MigrationOperationDescription{removeFieldOperationDescription},
			},
		}
	}
	return []*description.MigrationDescription{}
}

func (mf *MigrationFactory) factoryRemoveOuterLinkMigrationsForMeta(metaDescription *object_description.MetaDescription) []*description.MigrationDescription {
	spawnedMigrationDescriptions := make([]*description.MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		spawnedMigrationDescriptions = append(spawnedMigrationDescriptions, mf.factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription, &field)...)
	}
	return spawnedMigrationDescriptions
}

func (mf *MigrationFactory) factoryRemoveOuterLinkMigrationsForRemovedField(metaDescription *object_description.MetaDescription, field *object_description.Field) []*description.MigrationDescription {
	spawnedMigrationDescriptions := make([]*description.MigrationDescription, 0)
	if field.Type == object_description.FieldTypeObject && field.LinkType == object_description.LinkTypeInner {
		outerField := new(description_manager.MetaDescriptionManager).ReverseOuterField(metaDescription.Name, field, mf.metaDescriptionSyncer)
		removeFieldOperationDescription := description.MigrationOperationDescription{
			Type:  description.RemoveFieldOperation,
			Field: &description.MigrationFieldDescription{Field: *outerField, PreviousName: ""},
		}
		spawnedMigrationDescriptions = append(
			spawnedMigrationDescriptions,
			&description.MigrationDescription{
				ApplyTo:    field.LinkMeta,
				Operations: []description.MigrationOperationDescription{removeFieldOperationDescription},
			},
		)
	}
	return spawnedMigrationDescriptions
}

//check meta for generic inner links and spawn a migrations to create an outer link
func (mf *MigrationFactory) factoryAddGenericOuterLinkMigrationsForMeta(metaDescription *object_description.MetaDescription) ([]*description.MigrationDescription, error) {
	spawnedMigrations := make([]*description.MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		if migrations, err := mf.factoryAddGenericOuterLinkMigrationsForNewField(metaDescription.Name, &field); err != nil {
			return nil, err
		} else {
			spawnedMigrations = append(spawnedMigrations, migrations...)
		}
	}
	return spawnedMigrations, nil
}

//check if field is a generic inner link and spawn a migration to create an outer link
func (mf *MigrationFactory) factoryAddGenericOuterLinkMigrationsForNewField(metaName string, field *object_description.Field) ([]*description.MigrationDescription, error) {
	spawnedMigrations := make([]*description.MigrationDescription, 0)
	if field.Type == object_description.FieldTypeGeneric && field.LinkType == object_description.LinkTypeInner {
		//add reverse outer link for each referenced meta
		for _, linkMetaName := range field.LinkMetaList {
			fieldName := meta.ReverseInnerLinkName(metaName)
			linkMetaDescription, _, err := mf.metaDescriptionSyncer.Get(linkMetaName)
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
			addFieldOperationDescription := description.MigrationOperationDescription{
				Type:            description.AddFieldOperation,
				Field:           &description.MigrationFieldDescription{Field: outerField, PreviousName: ""},
				MetaDescription: linkMetaDescription,
			}

			spawnedMigrations = append(
				spawnedMigrations,
				&description.MigrationDescription{
					ApplyTo:    linkMetaDescription.Name,
					Operations: []description.MigrationOperationDescription{addFieldOperationDescription},
				},
			)
		}
	}
	return spawnedMigrations, nil
}

//check meta for generic inner links and spawn a migrations to update references to meta and their names if they were generated automatically
func (mf *MigrationFactory) factoryUpdateGenericOuterLinkMigrationsForMeta(currentMetaDescription *object_description.MetaDescription, newMetaDescription *object_description.MetaDescription) ([]*description.MigrationDescription, error) {
	spawnedMigrations := make([]*description.MigrationDescription, 0)
	for _, field := range currentMetaDescription.Fields {
		if field.Type == object_description.FieldTypeGeneric && field.LinkType == object_description.LinkTypeInner {
			for linkMetaName, outerField := range new(description_manager.MetaDescriptionManager).ReverseGenericOuterFields(currentMetaDescription.Name, &field, mf.metaDescriptionSyncer) {

				updatedField := outerField.Clone()
				updatedField.LinkMeta = newMetaDescription.Name

				if outerField.Name == meta.ReverseInnerLinkName(currentMetaDescription.Name) { //field is automatically generated
					updatedField.Name = meta.ReverseInnerLinkName(newMetaDescription.Name)
				}

				updateFieldOperationDescription := description.MigrationOperationDescription{
					Type:  description.UpdateFieldOperation,
					Field: &description.MigrationFieldDescription{Field: *updatedField, PreviousName: outerField.Name},
				}
				spawnedMigrations = append(spawnedMigrations, &description.MigrationDescription{
					ApplyTo:    linkMetaName,
					Operations: []description.MigrationOperationDescription{updateFieldOperationDescription},
				})
			}
		}
	}
	return spawnedMigrations, nil
}

//check if field is a generic inner link and spawn a migration to create or remove an outer link
//TODO: now it does not handle case of type`s change
func (mf *MigrationFactory) factoryAddGenericOuterLinkMigrationsForUpdatedField(metaName string, currentField *object_description.Field, newField *object_description.Field) ([]*description.MigrationDescription, []*description.MigrationDescription, error) {
	runAfter := make([]*description.MigrationDescription, 0)
	runBefore := make([]*description.MigrationDescription, 0)

	if currentField.Type == object_description.FieldTypeGeneric && currentField.LinkType == object_description.LinkTypeInner {
		if newField.Type == object_description.FieldTypeGeneric && newField.LinkType == object_description.LinkTypeInner {
			//if field has already been added

			excludedMetaNames := currentField.LinkMetaList.Diff(newField.LinkMetaList)
			includedMetaNames := newField.LinkMetaList.Diff(currentField.LinkMetaList)

			for _, excludedMetaName := range excludedMetaNames {
				outerField := object_description.Field{
					Name:           meta.ReverseInnerLinkName(metaName),
					Type:           object_description.FieldTypeGeneric,
					LinkType:       object_description.LinkTypeOuter,
					LinkMeta:       metaName,
					OuterLinkField: newField.Name,
					Optional:       true,
					QueryMode:      true,
					RetrieveMode:   false,
				}

				removeFieldOperationDescription := description.MigrationOperationDescription{
					Type:  description.RemoveFieldOperation,
					Field: &description.MigrationFieldDescription{Field: outerField, PreviousName: ""},
				}
				runBefore = append(
					runBefore,
					&description.MigrationDescription{
						ApplyTo:    excludedMetaName,
						Operations: []description.MigrationOperationDescription{removeFieldOperationDescription},
					},
				)
			}

			for _, includedMetaName := range includedMetaNames {
				outerField := object_description.Field{
					Name:           meta.ReverseInnerLinkName(metaName),
					Type:           object_description.FieldTypeGeneric,
					LinkType:       object_description.LinkTypeOuter,
					LinkMeta:       metaName,
					OuterLinkField: newField.Name,
					Optional:       true,
					QueryMode:      true,
					RetrieveMode:   false,
				}

				addFieldOperationDescription := description.MigrationOperationDescription{
					Type:  description.AddFieldOperation,
					Field: &description.MigrationFieldDescription{Field: outerField, PreviousName: ""},
				}
				runAfter = append(
					runAfter,
					&description.MigrationDescription{
						ApplyTo:    includedMetaName,
						Operations: []description.MigrationOperationDescription{addFieldOperationDescription},
					},
				)
			}
		} else {
			return nil, nil, migrations.NewMigrationError(migrations.MigrationErrorNotImplemented, "Generic inner link`s type change is not supported yet")
		}
	}

	return runBefore, runAfter, nil
}

func (mf *MigrationFactory) factoryRemoveGenericOuterLinkMigrationsForMeta(metaDescription *object_description.MetaDescription) []*description.MigrationDescription {
	spawnedMigrationDescriptions := make([]*description.MigrationDescription, 0)
	for _, field := range metaDescription.Fields {
		spawnedMigrationDescriptions = append(spawnedMigrationDescriptions, mf.factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription, &field)...)
	}
	return spawnedMigrationDescriptions
}

func (mf *MigrationFactory) factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription *object_description.MetaDescription, field *object_description.Field) []*description.MigrationDescription {
	spawnedMigrationDescriptions := make([]*description.MigrationDescription, 0)
	if field.Type == object_description.FieldTypeGeneric && field.LinkType == object_description.LinkTypeInner {
		for linkMetaName, outerFieldDescription := range new(description_manager.MetaDescriptionManager).ReverseGenericOuterFields(metaDescription.Name, field, mf.metaDescriptionSyncer) {
			removeFieldOperationDescription := description.MigrationOperationDescription{
				Type:  description.RemoveFieldOperation,
				Field: &description.MigrationFieldDescription{Field: *outerFieldDescription, PreviousName: ""},
			}
			spawnedMigrationDescriptions = append(
				spawnedMigrationDescriptions,
				&description.MigrationDescription{
					ApplyTo:    linkMetaName,
					Operations: []description.MigrationOperationDescription{removeFieldOperationDescription},
				},
			)
		}
	}
	return spawnedMigrationDescriptions
}

func NewMigrationFactory(metaStore *meta.MetaStore, globalTransaction *transactions.GlobalTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) *MigrationFactory {
	return &MigrationFactory{metaStore: metaStore, globalTransaction: globalTransaction, metaDescriptionSyncer: metaDescriptionSyncer}
}
