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
	switch concreteOperation := operation.(type) {
	case *object.CreateObjectOperation:
		runAfter, err := mf.factoryAddGenericOuterLinkMigrationsForMeta(concreteOperation.MetaDescription)
		return nil, runAfter, err
	case *field.AddFieldOperation:
		if runAfter, err := mf.factoryAddGenericOuterLinkMigrationsForNewField(metaDescription.Name, concreteOperation.Field); err != nil {
			return nil, nil, err
		} else {
			return nil, runAfter, nil
		}
	case *field.UpdateFieldOperation:
		return mf.factoryAddGenericOuterLinkMigrationsForUpdatedField(metaDescription.Name, concreteOperation.CurrentField, concreteOperation.NewField)
	case *object.RenameObjectOperation:
		runAfter, err := mf.factoryUpdateOuterLinkMigrationsForMeta(metaDescription, concreteOperation.MetaDescription)
		return nil, runAfter, err
	case *object.DeleteObjectOperation:
		runBefore := mf.factoryRemoveGenericOuterLinkMigrationsForMeta(metaDescription)
		return runBefore, nil, nil
	case *field.RemoveFieldOperation:
		runBefore := mf.factoryRemoveGenericOuterLinkMigrationsForRemovedField(metaDescription, concreteOperation.Field)
		return runBefore, nil, nil
	default:
		return nil, make([]*description.MigrationDescription, 0), nil
	}
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
				Field:           description.MigrationFieldDescription{Field: outerField, PreviousName: ""},
				MetaDescription: *linkMetaDescription,
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
func (mf *MigrationFactory) factoryUpdateOuterLinkMigrationsForMeta(currentMetaDescription *object_description.MetaDescription, newMetaDescription *object_description.MetaDescription) ([]*description.MigrationDescription, error) {
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
					Field: description.MigrationFieldDescription{Field: *updatedField, PreviousName: outerField.Name},
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
					Field: description.MigrationFieldDescription{Field: outerField, PreviousName: ""},
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
					Field: description.MigrationFieldDescription{Field: outerField, PreviousName: ""},
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
			return nil, nil, migrations.NewMigrationError("Generic inner link`s type change is not supported yet")
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
				Field: description.MigrationFieldDescription{Field: *outerFieldDescription, PreviousName: ""},
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
