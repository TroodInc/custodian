package meta

import (
	"encoding/json"
	"io"
	"logger"
	"server/errors"
	"utils"
	. "server/object/description"
	"server/transactions"
)

/*
   Metadata store of objects persisted in DB.
*/
type MetaStore struct {
	metaDescriptionSyncer    MetaDescriptionSyncer
	cache                    *MetaCache
	syncer                   MetaDbSyncer
	globalTransactionManager *transactions.GlobalTransactionManager
}

func (metaStore *MetaStore) UnmarshalIncomingJSON(r io.Reader) (*Meta, error) {
	var metaObj MetaDescription
	if e := json.NewDecoder(r).Decode(&metaObj); e != nil {
		return nil, errors.NewFatalError(ErrNotValid, "unmarshal", e.Error())
	}
	// normalize description
	return metaStore.NewMeta((&NormalizationService{}).Normalize(&metaObj))
}

func (metaStore *MetaStore) NewMeta(metaObj *MetaDescription) (*Meta, error) {
	return NewMetaFactory(metaStore.metaDescriptionSyncer).FactoryMeta(metaObj)
}

/*
   Gets the list of metadata objects from the underlying store.
*/
func (metaStore *MetaStore) List() ([]*MetaDescription, bool, error) {
	metaList, isFound, err := metaStore.metaDescriptionSyncer.List()

	if err != nil {
		return []*MetaDescription{}, isFound, err
	}

	return metaList, isFound, nil
}

/*
   Retrieves object metadata from the underlying store.
*/
func (metaStore *MetaStore) Get(name string, useCache bool) (*Meta, bool, error) {
	//try to get meta from cache
	if useCache {
		metaObj := metaStore.cache.Get(name)
		if metaObj != nil {
			return metaObj, true, nil
		}
	}

	//retrieve business object metadata from the storage
	metaData, isFound, err := metaStore.metaDescriptionSyncer.Get(name)

	if err != nil {
		return nil, isFound, err
	}
	//assemble the new business object with the given metadata
	metaObj, err := metaStore.NewMeta(metaData)
	if err != nil {
		return nil, isFound, err
	}

	transaction, _ := metaStore.globalTransactionManager.BeginTransaction(nil)
	//validate the newly created business object against existing one in the database
	ok, err := metaStore.syncer.ValidateObj(transaction.DbTransaction, metaObj.MetaDescription, metaStore.metaDescriptionSyncer)
	if ok {
		metaStore.cache.Set(metaObj)
		metaStore.globalTransactionManager.CommitTransaction(transaction)
		return metaObj, isFound, nil
	}
	metaStore.globalTransactionManager.RollbackTransaction(transaction)
	return nil, false, err
}

// Creates a new object type described by passed metadata.
func (metaStore *MetaStore) Create(objectMeta *Meta) error {
	transaction, _ := metaStore.globalTransactionManager.BeginTransaction(nil)

	if e := metaStore.syncer.CreateObj(transaction.DbTransaction, objectMeta.MetaDescription, metaStore.metaDescriptionSyncer); e == nil {
		if e := metaStore.metaDescriptionSyncer.Create(transaction.MetaDescriptionTransaction, *objectMeta.MetaDescription); e == nil {

			//add corresponding outer generic fields
			metaStore.addReversedOuterGenericFields(transaction, nil, objectMeta)
			//add corresponding outer field
			metaStore.addReversedOuterFields(transaction, nil, objectMeta)
			//create through object if needed
			if err := metaStore.createThroughMeta(transaction, objectMeta); err != nil {
				metaStore.globalTransactionManager.RollbackTransaction(transaction)
				return err
			}

			//invalidate cache
			metaStore.cache.Invalidate()
			metaStore.globalTransactionManager.CommitTransaction(transaction)
			return nil
		} else {
			var e2 = metaStore.syncer.RemoveObj(transaction.DbTransaction, objectMeta.Name, false)
			logger.Error("Error while compenstaion of object '%s' metadata creation: %v", objectMeta.Name, e2)
			metaStore.globalTransactionManager.RollbackTransaction(transaction)
			return e
		}
	} else {
		metaStore.globalTransactionManager.RollbackTransaction(transaction)
		return e
	}
}

// Updates an existing object metadata.
func (metaStore *MetaStore) Update(name string, newMetaObj *Meta, keepOuter bool) (bool, error) {
	globalTransaction, _ := metaStore.globalTransactionManager.BeginTransaction(nil)
	if currentMetaObj, ok, err := metaStore.Get(name, false); err == nil {
		if keepOuter {
			metaStore.processGenericOuterLinkKeeping(globalTransaction, currentMetaObj, newMetaObj)
		}
		// remove possible outer links before main update processing

		metaStore.processInnerLinksRemoval(globalTransaction, currentMetaObj, newMetaObj)
		metaStore.processGenericInnerLinksRemoval(globalTransaction, currentMetaObj, newMetaObj)

		ok, e := metaStore.metaDescriptionSyncer.Update(name, *newMetaObj.MetaDescription)

		if e != nil || !ok {
			metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
			return ok, e
		}

		if updateError := metaStore.syncer.UpdateObj(globalTransaction.DbTransaction, currentMetaObj.MetaDescription, newMetaObj.MetaDescription, metaStore.metaDescriptionSyncer); updateError == nil {
			//add corresponding outer generic fields
			metaStore.addReversedOuterGenericFields(globalTransaction, currentMetaObj, newMetaObj)

			//add corresponding outer field
			metaStore.addReversedOuterFields(globalTransaction, currentMetaObj, newMetaObj)

			//create through object if needed
			if err := metaStore.createThroughMeta(globalTransaction, newMetaObj); err != nil {
				metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
				return false, err
			}

			//invalidate cache
			metaStore.cache.Invalidate()
			metaStore.globalTransactionManager.CommitTransaction(globalTransaction)
			return true, nil
		} else {
			//rollback to the previous version
			rollbackError := metaStore.syncer.UpdateObjTo(globalTransaction.DbTransaction, currentMetaObj.MetaDescription, metaStore.metaDescriptionSyncer)
			if rollbackError != nil {
				logger.Error("Error while rolling back an update of MetaDescription '%s': %v", name, rollbackError)
				metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
				return false, updateError

			}
			_, rollbackError = metaStore.metaDescriptionSyncer.Update(name, *currentMetaObj.MetaDescription)
			if rollbackError != nil {
				logger.Error("Error while rolling back an update of MetaDescription '%s': %v", name, rollbackError)
				metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
				return false, updateError
			}
			return false, updateError
		}
	} else {
		metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
		return ok, err
	}
}

// Deletes an existing object metadata from the store.
func (metaStore *MetaStore) Remove(transaction *transactions.GlobalTransaction, name string, force bool) (bool, error) {

	meta, _, err := metaStore.Get(name, false)
	if err != nil {
		return false, err
	}
	//remove related links from the database
	metaStore.removeRelatedOuterLinks(transaction, meta)
	metaStore.removeRelatedInnerLinks(transaction, meta)
	metaStore.removeRelatedObjectsFieldAndThroughMeta(transaction, force, meta)

	//remove related generic links from the database
	metaStore.removeRelatedGenericOuterLinks(transaction, meta)
	metaStore.removeRelatedGenericInnerLinks(transaction, meta)

	//remove object from the database
	if e := metaStore.syncer.RemoveObj(transaction.DbTransaction, name, force); e == nil {
		//remove object`s description *.json file
		ok, err := metaStore.metaDescriptionSyncer.Remove(name)

		//invalidate cache
		metaStore.cache.Invalidate()

		return ok, err
	} else {
		return false, e
	}
}

//Remove all outer fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedOuterLinks(transaction *transactions.GlobalTransaction, targetMeta *Meta) {
	for _, field := range targetMeta.Fields {
		if field.Type == FieldTypeObject && field.LinkType == LinkTypeInner {
			metaStore.removeRelatedOuterLink(transaction, targetMeta, field)
		}
	}
}

//Remove outer field from related object if it links to the given field
func (metaStore *MetaStore) removeRelatedOuterLink(transaction *transactions.GlobalTransaction, targetMeta *Meta, innerLinkFieldDescription FieldDescription) {
	relatedObjectMeta := innerLinkFieldDescription.LinkMeta
	for i, relatedObjectField := range relatedObjectMeta.Fields {
		if relatedObjectField.LinkType == LinkTypeOuter &&
			relatedObjectField.LinkMeta.Name == targetMeta.Name &&
			relatedObjectField.OuterLinkField.Field.Name == innerLinkFieldDescription.Field.Name {
			//omit outer field and update related object
			relatedObjectMeta.Fields = append(relatedObjectMeta.Fields[:i], relatedObjectMeta.Fields[i+1:]...)
			relatedObjectMeta.MetaDescription.Fields = append(relatedObjectMeta.MetaDescription.Fields[:i], relatedObjectMeta.MetaDescription.Fields[i+1:]...)
			metaStore.Update(relatedObjectMeta.Name, relatedObjectMeta, true)
		}
	}
}

//Remove all outer fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedGenericOuterLinks(transaction *transactions.GlobalTransaction, targetMeta *Meta) {
	for _, field := range targetMeta.Fields {
		if field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner {
			metaStore.removeRelatedToInnerGenericOuterLinks(transaction, targetMeta, field, field.LinkMetaList.GetAll())
		}
	}
}

//Remove generic outer field from each of linkMetaList`s meta related to the given inner generic field
func (metaStore *MetaStore) removeRelatedToInnerGenericOuterLinks(transaction *transactions.GlobalTransaction, targetMeta *Meta, genericInnerLinkFieldDescription FieldDescription, linkMetaList []*Meta) {
	for _, relatedObjectMeta := range linkMetaList {
		for i, relatedObjectField := range relatedObjectMeta.Fields {
			if relatedObjectField.Type == FieldTypeGeneric &&
				relatedObjectField.LinkType == LinkTypeOuter &&
				relatedObjectField.LinkMeta.Name == targetMeta.Name &&
				relatedObjectField.OuterLinkField.Field.Name == genericInnerLinkFieldDescription.Field.Name {
				//omit outer field and update related object
				relatedObjectMeta.Fields = append(relatedObjectMeta.Fields[:i], relatedObjectMeta.Fields[i+1:]...)
				relatedObjectMeta.MetaDescription.Fields = append(relatedObjectMeta.MetaDescription.Fields[:i], relatedObjectMeta.MetaDescription.Fields[i+1:]...)
				metaStore.Update(relatedObjectMeta.Name, relatedObjectMeta, false)
			}
		}
	}
}

//Remove inner fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedInnerLinks(transaction *transactions.GlobalTransaction, targetMeta *Meta) {
	metaDescriptionList, _, _ := metaStore.List()
	for _, objectMetaDescription := range metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name, false)
			objectMetaFields := make([]Field, 0)
			objectMetaFieldDescriptions := make([]FieldDescription, 0)
			objectNeedsUpdate := false

			for i, fieldDescription := range objectMeta.Fields {
				//omit orphan fields
				fieldIsTargetInnerLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeObject && fieldDescription.LinkMeta.Name == targetMeta.Name
				fieldIsTargetOuterLink := fieldDescription.LinkType == LinkTypeOuter && fieldDescription.Type == FieldTypeArray && fieldDescription.LinkMeta.Name == targetMeta.Name

				if !(fieldIsTargetInnerLink || fieldIsTargetOuterLink) {
					objectMetaFields = append(objectMetaFields, objectMeta.MetaDescription.Fields[i])
					objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, objectMeta.Fields[i])
				} else {
					objectNeedsUpdate = true
				}
			}
			// it means that related object should be updated
			if objectNeedsUpdate {
				objectMeta.Fields = objectMetaFieldDescriptions
				objectMeta.MetaDescription.Fields = objectMetaFields
				metaStore.Update(objectMeta.Name, objectMeta, true)
			}
		}
	}
}

//Remove inner fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedGenericInnerLinks(transaction *transactions.GlobalTransaction, targetMeta *Meta) {
	metaDescriptionList, _, _ := metaStore.List()
	for _, objectMetaDescription := range metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name, false)
			objectMetaFields := make([]Field, 0)
			objectMetaFieldDescriptions := make([]FieldDescription, 0)
			objectNeedsUpdate := false

			for i, fieldDescription := range objectMeta.Fields {
				//omit orphan fields
				fieldIsTargetGenericInnerLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeGeneric && utils.Contains(fieldDescription.Field.LinkMetaList, targetMeta.Name)

				if !fieldIsTargetGenericInnerLink {
					objectMetaFields = append(objectMetaFields, objectMeta.MetaDescription.Fields[i])
					objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, objectMeta.Fields[i])
				} else {
					objectNeedsUpdate = true
					indexOfTargetMeta := utils.IndexOf(fieldDescription.Field.LinkMetaList, targetMeta.Name)

					//alter field
					field := objectMeta.MetaDescription.Fields[i]
					field.LinkMetaList = append(field.LinkMetaList[:indexOfTargetMeta], field.LinkMetaList[indexOfTargetMeta+1:]...)
					objectMetaFields = append(objectMetaFields, field)

					//alter field description
					fieldDescription := objectMeta.Fields[i]
					fieldDescription.LinkMetaList.RemoveByName(targetMeta.Name)
					objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, fieldDescription)

				}
			}
			// it means that related object should be updated
			if objectNeedsUpdate {
				objectMeta.Fields = objectMetaFieldDescriptions
				objectMeta.MetaDescription.Fields = objectMetaFields
				metaStore.Update(objectMeta.Name, objectMeta, true)
			}
		}
	}
}

//Remove inner fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedObjectsFieldAndThroughMeta(transaction *transactions.GlobalTransaction, keepMeta bool, targetMeta *Meta) error {
	metaDescriptionList, _, _ := metaStore.List()
	for _, objectMetaDescription := range metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name, false)
			objectMetaFields := make([]Field, 0)
			objectMetaFieldDescriptions := make([]FieldDescription, 0)
			objectNeedsUpdate := false

			for i, fieldDescription := range objectMeta.Fields {
				//omit orphan fields
				fieldIsObjectsLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeObjects

				if !fieldIsObjectsLink {
					objectMetaFields = append(objectMetaFields, objectMeta.MetaDescription.Fields[i])
					objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, objectMeta.Fields[i])
				} else {
					objectNeedsUpdate = true
					if !keepMeta {
						if _, err := metaStore.Remove(transaction, fieldDescription.LinkThrough.Name, true); err != nil {
							return err
						}
					}
				}
			}

			// means that related object should be updated
			if objectNeedsUpdate {
				objectMeta.Fields = objectMetaFieldDescriptions
				objectMeta.MetaDescription.Fields = objectMetaFields
				if _, err := metaStore.Update(objectMeta.Name, objectMeta, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// compare current object`s version to the version is being updated and remove outer links
// if any inner link is being removed
func (metaStore *MetaStore) processInnerLinksRemoval(transaction *transactions.GlobalTransaction, currentMeta *Meta, metaToBeUpdated *Meta) {
	for _, currentFieldDescription := range currentMeta.Fields {
		if currentFieldDescription.LinkType == LinkTypeInner && currentFieldDescription.Type == FieldTypeObject {
			fieldIsBeingRemoved := true
			for _, fieldDescriptionToBeUpdated := range metaToBeUpdated.Fields {
				if fieldDescriptionToBeUpdated.Name == currentFieldDescription.Name &&
					fieldDescriptionToBeUpdated.LinkType == LinkTypeInner &&
					fieldDescriptionToBeUpdated.Type == currentFieldDescription.Type &&
					fieldDescriptionToBeUpdated.LinkMeta.Name == currentFieldDescription.LinkMeta.Name {
					fieldIsBeingRemoved = false
				}
			}
			if fieldIsBeingRemoved {
				metaStore.removeRelatedOuterLink(transaction, currentMeta, currentFieldDescription)
			}
		}
	}
}

// compare current object`s version to the version is being updated and remove outer links
// if any generic inner link is being removed
func (metaStore *MetaStore) processGenericInnerLinksRemoval(transaction *transactions.GlobalTransaction, currentMeta *Meta, metaToBeUpdated *Meta) {
	for _, currentFieldDescription := range currentMeta.Fields {
		if currentFieldDescription.LinkType == LinkTypeInner && currentFieldDescription.Type == FieldTypeGeneric {
			fieldIsBeingRemoved := true
			fieldIsBeingUpdated := true
			var linkMetaListDiff []*Meta
			for _, fieldDescriptionToBeUpdated := range metaToBeUpdated.Fields {
				if fieldDescriptionToBeUpdated.Name == currentFieldDescription.Name &&
					fieldDescriptionToBeUpdated.LinkType == LinkTypeInner {
					if utils.Equal(fieldDescriptionToBeUpdated.Field.LinkMetaList, currentFieldDescription.Field.LinkMetaList, false) {
						fieldIsBeingRemoved = false
						fieldIsBeingUpdated = false
					} else {
						fieldIsBeingRemoved = false
						fieldIsBeingUpdated = true
						linkMetaListDiff = currentFieldDescription.LinkMetaList.Diff(fieldDescriptionToBeUpdated.LinkMetaList.GetAll())
					}
				}
			}
			//process generic outer link removal only for removed metas
			if fieldIsBeingUpdated {
				metaStore.removeRelatedToInnerGenericOuterLinks(transaction, currentMeta, currentFieldDescription, linkMetaListDiff)
			}
			//process generic outer link removal for each linked meta
			if fieldIsBeingRemoved {
				metaStore.removeRelatedToInnerGenericOuterLinks(transaction, currentMeta, currentFieldDescription, currentFieldDescription.LinkMetaList.GetAll())
			}
		}
	}
}

//track outer link removal and return it if corresponding inner generic field was not removed
//do nothing if field is being renamed
func (metaStore *MetaStore) processGenericOuterLinkKeeping(transaction *transactions.GlobalTransaction, previousMeta *Meta, currentMeta *Meta) {
	for _, previousMetaField := range previousMeta.Fields {
		if previousMetaField.Type == FieldTypeGeneric && previousMetaField.LinkType == LinkTypeOuter {
			if currentMeta.FindField(previousMetaField.Name) == nil {
				if _, _, err := metaStore.Get(previousMetaField.LinkMeta.Name, false); err == nil {
					fieldIsBeingRenamed := false
					for _, currentMetaField := range currentMeta.Fields {
						if currentMetaField.Type == FieldTypeGeneric && currentMetaField.LinkType == LinkTypeOuter {
							if currentMetaField.LinkMeta.Name == previousMetaField.LinkMeta.Name {
								fieldIsBeingRenamed = true
							}
						}
					}
					if !fieldIsBeingRenamed {
						previousMetaField.RetrieveMode = false
						previousMetaField.QueryMode = true
						currentMeta.AddField(previousMetaField)
					}
				}
			}
		}
	}
}

//add corresponding reverse outer generic field if field is added
func (metaStore *MetaStore) addReversedOuterGenericFields(transaction *transactions.GlobalTransaction, previousMeta *Meta, currentMeta *Meta) {
	for _, field := range currentMeta.Fields {
		if field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner {
			shouldProcessOuterLinks := true
			var excludedMetas []*Meta
			var includedMetas []*Meta

			if previousMeta != nil {
				previousStateField := previousMeta.FindField(field.Name)
				if previousStateField != nil {
					//if field has already been added
					if previousStateField.LinkType == field.LinkType && previousStateField.Type == field.Type {
						shouldProcessOuterLinks = false
						if excludedMetas = previousStateField.LinkMetaList.Diff(field.LinkMetaList.metas); len(excludedMetas) > 0 {
							shouldProcessOuterLinks = true
						}
						if includedMetas = field.LinkMetaList.Diff(previousStateField.LinkMetaList.metas); len(includedMetas) > 0 {
							shouldProcessOuterLinks = true
						}
					}
				} else {
					includedMetas = field.LinkMetaList.metas
				}
			} else {
				includedMetas = field.LinkMetaList.metas
			}

			if shouldProcessOuterLinks {
				//add reverse outer
				for _, linkMeta := range includedMetas {
					fieldName := ReverseInnerLinkName(currentMeta.Name)
					if linkMeta.FindField(fieldName) != nil {
						continue
					}
					outerField := Field{
						Name:           fieldName,
						Type:           FieldTypeGeneric,
						LinkType:       LinkTypeOuter,
						LinkMeta:       currentMeta.Name,
						OuterLinkField: field.Name,
						Optional:       true,
						QueryMode:      true,
						RetrieveMode:   false,
					}
					linkMeta.MetaDescription.Fields = append(
						linkMeta.MetaDescription.Fields,
						outerField,
					)
					linkMeta.Fields = append(linkMeta.Fields,
						FieldDescription{
							Field:          &outerField,
							Meta:           linkMeta,
							LinkMeta:       currentMeta,
							OuterLinkField: &field,},
					)
					metaStore.Update(linkMeta.Name, linkMeta, true)
				}

				//remove reverse outer
				for _, excludedMeta := range excludedMetas {
					for i, excludedField := range excludedMeta.Fields {
						if excludedField.Type == FieldTypeGeneric && excludedField.LinkType == LinkTypeOuter {
							if excludedField.OuterLinkField.Name == field.Name && excludedField.LinkMeta.Name == field.Meta.Name {
								excludedMeta.Fields = append(excludedMeta.Fields[:i], excludedMeta.Fields[i+1:]...)
								excludedMeta.MetaDescription.Fields = append(excludedMeta.MetaDescription.Fields[:i], excludedMeta.MetaDescription.Fields[i+1:]...)
								metaStore.Update(excludedMeta.Name, excludedMeta, true)
							}
						}
					}
				}
			}
		}
	}
}

//add corresponding reverse outer generic field if field is added
func (metaStore *MetaStore) addReversedOuterFields(transaction *transactions.GlobalTransaction, previousMeta *Meta, currentMeta *Meta) {
	for _, field := range currentMeta.Fields {
		if field.Type == FieldTypeObject && field.LinkType == LinkTypeInner {
			var referencedMeta *Meta
			if previousMeta != nil {
				previousStateField := previousMeta.FindField(field.Name)
				if previousStateField != nil {
					//if field has already been added
					//TODO: remove referencedMeta = field.LinkMeta outside of "if" block, this is a temporary workaround
					//to provide backward compatibility with Topline schema
					referencedMeta = field.LinkMeta
					if previousStateField.LinkType != field.LinkType ||
						previousStateField.Type != field.Type ||
						previousStateField.LinkMeta.Name != field.LinkMeta.Name {
						referencedMeta = field.LinkMeta
					}
				} else {
					referencedMeta = field.LinkMeta
				}
			} else {
				referencedMeta = field.LinkMeta
			}

			if referencedMeta != nil {
				//add reverse outer
				fieldName := ReverseInnerLinkName(currentMeta.Name)
				if referencedMeta.FindField(fieldName) != nil {
					continue
				}
				//automatically added outer field should only be available for querying
				outerField := Field{
					Name:           fieldName,
					Type:           FieldTypeArray,
					LinkType:       LinkTypeOuter,
					LinkMeta:       currentMeta.Name,
					OuterLinkField: field.Name,
					Optional:       true,
					QueryMode:      true,
					RetrieveMode:   false,
				}
				referencedMeta.MetaDescription.Fields = append(
					referencedMeta.MetaDescription.Fields,
					outerField,
				)
				referencedMeta.Fields = append(referencedMeta.Fields,
					FieldDescription{
						Field:          &outerField,
						Meta:           referencedMeta,
						LinkMeta:       currentMeta,
						OuterLinkField: &field,
					},
				)
				metaStore.Update(referencedMeta.Name, referencedMeta, true)
			}
		}
	}
}

func (metaStore *MetaStore) Cache() *MetaCache {
	return metaStore.cache
}

//create through meta if it does not exist
func (metaStore *MetaStore) createThroughMeta(transaction *transactions.GlobalTransaction, meta *Meta) error {
	for _, field := range meta.Fields {
		if field.Type == FieldTypeObjects {
			if _, _, err := metaStore.Get(field.LinkThrough.Name, false); err != nil {
				return metaStore.Create(field.LinkThrough)
			}
		}
	}
	return nil
}

// Updates an existing object metadata.
func (metaStore *MetaStore) Flush(globalTransaction *transactions.GlobalTransaction) error {
	metaList, _, err := metaStore.List()
	if err != nil {
		return err
	}
	for _, meta := range metaList {
		if _, err := metaStore.Remove(globalTransaction, meta.Name, true); err != nil {
			return err
		}
	}
	return nil
}

func NewStore(md MetaDescriptionSyncer, mds MetaDbSyncer, gtm *transactions.GlobalTransactionManager) *MetaStore {
	return &MetaStore{metaDescriptionSyncer: md, syncer: mds, cache: NewCache(), globalTransactionManager: gtm}
}
