package validation

import (
	"server/pg/migrations/managers"
	"server/transactions"
	"utils"
	_migrations "server/migrations"
	"server/migrations/storage"
	"reflect"
	"server/migrations/description"
	"fmt"
)

type MigrationValidationService struct {
	migrationManager *managers.MigrationManager
	migrationStorage *storage.MigrationStorage
}

//check if the given migration has no conflicts with already applied migrations
//check if migration`s list of parents has not changed, so the given migration definitely will not cause conflicts
func (mv *MigrationValidationService) Validate(migrationDescription *description.MigrationDescription, transaction transactions.DbTransaction) error {
	metaName, err := migrationDescription.MetaName()
	if err != nil {
		return err
	}

	appliedParentMigrations, err := mv.migrationManager.GetPrecedingMigrationsForObject(metaName, transaction)
	if err != nil {
		return err
	}

	siblingMigrationsIds := make([]string, 0)
	for _, migration := range appliedParentMigrations {
		siblingMigrationsIds = append(siblingMigrationsIds, migration.Data["migration_id"].(string))
	}

	//case 0: the given migration has no any applied siblings and its direct parents are the latest applied migrations for the given migration`s object
	if !utils.Equal(siblingMigrationsIds, migrationDescription.DependsOn, false) {
		//case 1: the given migration was constructed, but another migrations were applied as its parents` siblings
		if len(utils.Intersection(siblingMigrationsIds, migrationDescription.DependsOn)) > 0 {
			return _migrations.NewMigrationError(_migrations.MigrationErrorParentsChanged, "The given migration`s parents` list has changed since this migration was constructed")
		}

		latestMigrationDescription, err := mv.migrationStorage.Get(siblingMigrationsIds[0])
		if err != nil {
			return err
		}
		if reflect.DeepEqual(migrationDescription.DependsOn, latestMigrationDescription.DependsOn) {
			if len(migrationDescription.DependsOn) == 0 {
				//	case : candidate migration is an attempt to create already existing object
				return _migrations.NewMigrationError(_migrations.MigrationIsNotActual, "The given migration is supposed to create an already existing object")
			} else {
				//case 1: latest applied migrations have the same parents, therefore they are siblings to the given migration
				return mv.validateMigrationAndItsSiblings(migrationDescription, siblingMigrationsIds)
			}
		} else {
			//case 2: latest applied migrations, having the same parents, are not equal to migration`s parents. Migration
			// history of the given migration`s object has went further than migration`s description assumes and it is
			// no more actual
			return _migrations.NewMigrationError(_migrations.MigrationIsNotActual, "The given migration is outdated and the migration history of its object has significantly changed")
		}
	} else {
		return nil
	}
}

func (mv *MigrationValidationService) validateMigrationAndItsSiblings(migrationDescription *description.MigrationDescription, siblingIds []string) error {
	//validate the candidate itself
	if err := mv.validateMigrationHavingSiblings(migrationDescription); err != nil {
		return err
	}
	for _, siblingId := range siblingIds {
		siblingMigrationDescription, err := mv.migrationStorage.Get(siblingId)
		if err != nil {
			return err
		}
		//validate the sibling itself
		if err := mv.validateMigrationHavingSiblings(siblingMigrationDescription); err != nil {
			return err
		}
		//validate the candidate against the sibling
		if err := mv.validateMigrationAgainstSingleSibling(migrationDescription, siblingMigrationDescription); err != nil {
			return err
		}
	}
	return nil
}

//migration which is supposed to be applied along with its siblings cannot have any object-related operations and cannot rename any field
//each of siblings should pass this validation
func (mv *MigrationValidationService) validateMigrationHavingSiblings(migrationDescription *description.MigrationDescription) error {
	for _, operation := range migrationDescription.Operations {
		if operation.Type == description.RenameObjectOperation || operation.Type == description.DeleteObjectOperation {
			return _migrations.NewMigrationError(
				_migrations.MigrationIsNotCompatibleWithSiblings,
				fmt.Sprintln("Migration", migrationDescription.Id, "contains object operation(s) and cannot be applied along with siblings"),
			)
		}
		if operation.Type == description.UpdateFieldOperation {
			if operation.Field.PreviousName != operation.Field.Name {
				return _migrations.NewMigrationError(
					_migrations.MigrationIsNotCompatibleWithSiblings,
					fmt.Sprintln("Migration", migrationDescription.Id, "contains operation which renames the field", operation.Field.PreviousName, "and cannot be applied along with siblings"),
				)
			}
		}
	}
	return nil
}

//migrations having the same parents cannot modify the same field or action
func (mv *MigrationValidationService) validateMigrationAgainstSingleSibling(migrationDescription, siblingMigrationDescription *description.MigrationDescription) error {
	for _, operation := range migrationDescription.Operations {
		for _, siblingOperation := range siblingMigrationDescription.Operations {
			if siblingOperation.Type == description.UpdateFieldOperation || siblingOperation.Type == description.RemoveFieldOperation || siblingOperation.Type == description.AddFieldOperation {
				if operation.Type == description.UpdateFieldOperation || operation.Type == description.RemoveFieldOperation || operation.Type == description.AddFieldOperation {
					if operation.Field.Name == siblingOperation.Field.Name {
						return _migrations.NewMigrationError(
							_migrations.MigrationIsNotCompatibleWithSiblings,
							fmt.Sprintln("Migration", migrationDescription.Id, "contains the operation on the same field", operation.Field.Name, "as the migration", siblingMigrationDescription.Id, "has and cannot be applied along with it"),
						)
					}
				}
			}
			if siblingOperation.Type == description.UpdateActionOperation || siblingOperation.Type == description.RemoveActionOperation || siblingOperation.Type == description.AddActionOperation {
				if operation.Type == description.UpdateActionOperation || operation.Type == description.RemoveActionOperation || operation.Type == description.AddActionOperation {
					if operation.Action.Name == siblingOperation.Action.Name {
						return _migrations.NewMigrationError(
							_migrations.MigrationIsNotCompatibleWithSiblings,
							fmt.Sprintln("Migration", migrationDescription.Id, "contains the operation on the same action", operation.Action.Name, "as the migration", siblingMigrationDescription.Id, "has and cannot be applied along with it"),
						)
					}
				}
			}
		}
	}
	return nil
}

func NewMigrationValidationService(manager *managers.MigrationManager, migrationStoragePath string) *MigrationValidationService {
	return &MigrationValidationService{migrationManager: manager, migrationStorage: storage.NewMigrationStorage(migrationStoragePath)}
}