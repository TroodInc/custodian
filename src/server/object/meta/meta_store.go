package meta

import (
	"encoding/json"
	"io"
	"logger"
	"server/data/notifications"
	"server/errors"
	"server/transactions"
	"utils"
)

/*
   Metadata store of objects persisted in DB.
*/
type MetaStore struct {
	MetaDescriptionSyncer    MetaDescriptionSyncer
	cache                    *MetaCache
	Syncer                   MetaDbSyncer
	globalTransactionManager *transactions.GlobalTransactionManager
}

func (metaStore *MetaStore) UnmarshalIncomingJSON(r io.Reader) (*Meta, error) {
	var metaObj Meta
	if e := json.NewDecoder(r).Decode(&metaObj); e != nil {
		return nil, errors.NewFatalError(ErrNotValid, "unmarshal", e.Error())
	}
	// normalize description
	return metaStore.NewMeta((&NormalizationService{}).Normalize(&metaObj))
}

func (metaStore *MetaStore) NewMeta(metaObj *Meta) (*Meta, error) {
	return NewMetaFactory(metaStore.MetaDescriptionSyncer).FactoryMeta(metaObj)
}

/*
   Gets the list of metadata objects from the underlying store.
*/
func (metaStore *MetaStore) List() []*Meta {
	result := make([]*Meta, 0)

	if metaMapList, _, err := metaStore.MetaDescriptionSyncer.List(); err == nil {
		for _, metaMap := range metaMapList {
			result = append(result, NewMetaFromMap(metaMap))
		}
	}

	return result
}


func (metaStore *MetaStore) GetActions() map[string][]*notifications.Action {
	actions := map[string][]*notifications.Action{}
	for _, meta := range metaStore.List() {
		actions[meta.Name] = meta.Actions
	}

	return actions
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
	metaData, isFound, err := metaStore.MetaDescriptionSyncer.Get(name)

	if err != nil {
		return nil, isFound, err
	}
	//assemble the new business object with the given metadata
	metaObj, err := metaStore.NewMeta(NewMetaFromMap(metaData))
	if err != nil {
		return nil, isFound, err
	}

	metaStore.cache.Set(metaObj)
	return metaObj, isFound, nil

	// @todo: Find another way of checking DB schema consistency
	//transaction, _ := metaStore.globalTransactionManager.BeginTransaction(nil)
	//validate the newly created business object against existing one in the database
	//ok, err := metaStore.Syncer.ValidateObj(transaction.DbTransaction, metaObj.MetaDescription, metaStore.MetaDescriptionSyncer)
	//if ok {
	//	metaStore.cache.Set(metaObj)
	//	metaStore.globalTransactionManager.CommitTransaction(transaction)
	//	return metaObj, isFound, nil
	//}
	//metaStore.globalTransactionManager.RollbackTransaction(transaction)
	//return nil, false, err
}

// Creates a new object type described by passed metadata.
func (metaStore *MetaStore) Create(objectMeta *Meta) error {
	transaction, _ := metaStore.globalTransactionManager.BeginTransaction()

	if e := metaStore.Syncer.CreateObj(transaction.DbTransaction, objectMeta, metaStore.MetaDescriptionSyncer); e == nil {
		if e := metaStore.MetaDescriptionSyncer.Create(transaction.MetaDescriptionTransaction, objectMeta.Name, objectMeta.ForExport()); e == nil {

			//add corresponding outer generic fields
			metaStore.addReversedOuterGenericFields(nil, objectMeta)
			//add corresponding outer field
			metaStore.addReversedOuterFields(nil, objectMeta)
			//create through object if needed
			if err := metaStore.createThroughMeta(objectMeta); err != nil {
				metaStore.globalTransactionManager.RollbackTransaction(transaction)
				return err
			}

			//invalidate cache
			metaStore.cache.Invalidate()
			metaStore.globalTransactionManager.CommitTransaction(transaction)
			return nil
		} else {
			var e2 = metaStore.Syncer.RemoveObj(transaction.DbTransaction, objectMeta.Name, false)
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
	if currentMetaObj, ok, err := metaStore.Get(name, false); err == nil {
		if keepOuter {
			metaStore.processGenericOuterLinkKeeping(currentMetaObj, newMetaObj)
		}
		// remove possible outer links before main update processing

		metaStore.processInnerLinksRemoval(currentMetaObj, newMetaObj)
		metaStore.processGenericInnerLinksRemoval(currentMetaObj, newMetaObj)

		ok, e := metaStore.MetaDescriptionSyncer.Update(name, newMetaObj.ForExport())

		if e != nil || !ok {
			return ok, e
		}

		globalTransaction, _ := metaStore.globalTransactionManager.BeginTransaction()
		if updateError := metaStore.Syncer.UpdateObj(globalTransaction.DbTransaction, currentMetaObj, newMetaObj, metaStore.MetaDescriptionSyncer); updateError == nil {
			//add corresponding outer generic fields
			metaStore.addReversedOuterGenericFields(currentMetaObj, newMetaObj)

			//add corresponding outer field
			metaStore.addReversedOuterFields(currentMetaObj, newMetaObj)

			//create through object if needed
			if err := metaStore.createThroughMeta(newMetaObj); err != nil {
				metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
				return false, err
			}

			//invalidate cache
			metaStore.cache.Invalidate()
			metaStore.globalTransactionManager.CommitTransaction(globalTransaction)
			return true, nil
		} else {
			//rollback to the previous version
			rollbackError := metaStore.Syncer.UpdateObjTo(globalTransaction.DbTransaction, currentMetaObj, metaStore.MetaDescriptionSyncer)
			if rollbackError != nil {
				logger.Error("Error while rolling back an update of MetaDescription '%s': %v", name, rollbackError)
				metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
				return false, updateError

			}
			_, rollbackError = metaStore.MetaDescriptionSyncer.Update(name, currentMetaObj.ForExport())
			if rollbackError != nil {
				logger.Error("Error while rolling back an update of MetaDescription '%s': %v", name, rollbackError)
				metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
				return false, updateError
			}
			metaStore.globalTransactionManager.RollbackTransaction(globalTransaction)
			return false, updateError
		}
	} else {
		return ok, err
	}
}

// Deletes an existing object metadata from the store.
func (metaStore *MetaStore) Remove(name string, force bool) (bool, error) {
	meta, _, err := metaStore.Get(name, false)
	if err != nil {
		return false, err
	}
	//remove related links from the database
	metaStore.removeRelatedOuterLinks(meta)
	metaStore.removeRelatedInnerLinks(meta)
	err = metaStore.removeRelatedObjectsFieldAndThroughMeta(force, meta)

	//remove related generic links from the database
	metaStore.removeRelatedGenericOuterLinks(meta)
	metaStore.removeRelatedGenericInnerLinks(meta)

	//remove object from the database
	transaction, _ := metaStore.globalTransactionManager.BeginTransaction()
	if e := metaStore.Syncer.RemoveObj(transaction.DbTransaction, name, force); e == nil {
		//remove object`s description *.json file
		ok, err := metaStore.MetaDescriptionSyncer.Remove(name)

		//invalidate cache
		metaStore.cache.Invalidate()
		metaStore.globalTransactionManager.CommitTransaction(transaction)

		return ok, err
	} else {
		metaStore.globalTransactionManager.RollbackTransaction(transaction)
		return false, e
	}
}

//Remove all outer fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedOuterLinks(targetMeta *Meta) {
	for _, field := range targetMeta.Fields {
		if field.Type == FieldTypeObject && field.LinkType == LinkTypeInner {
			metaStore.removeRelatedOuterLink(targetMeta, field)
		}
	}
}

//Remove outer field from related object if it links to the given field
func (metaStore *MetaStore) removeRelatedOuterLink(targetMeta *Meta, innerLinkFieldDescription *Field) {
	relatedObjectMeta := innerLinkFieldDescription.LinkMeta
	for i, relatedObjectField := range relatedObjectMeta.Fields {
		if relatedObjectField.LinkType == LinkTypeOuter &&
			relatedObjectField.LinkMeta.Name == targetMeta.Name &&
			relatedObjectField.OuterLinkField.Name == innerLinkFieldDescription.Name {
			//omit outer field and update related object
			delete(relatedObjectMeta.Fields, i)
			metaStore.Update(relatedObjectMeta.Name, relatedObjectMeta, true)
		}
	}
}

//Remove all outer fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedGenericOuterLinks(targetMeta *Meta) {
	for _, field := range targetMeta.Fields {
		if field.Type == FieldTypeGeneric && field.LinkType == LinkTypeInner {
			metaStore.removeRelatedToInnerGenericOuterLinks(targetMeta, field, field.LinkMetaList)
		}
	}
}

//Remove generic outer field from each of linkMetaList`s meta related to the given inner generic field
func (metaStore *MetaStore) removeRelatedToInnerGenericOuterLinks(targetMeta *Meta, genericInnerLinkFieldDescription *Field, linkMetaList []*Meta) {
	for _, relatedObjectMeta := range linkMetaList {
		for i, relatedObjectField := range relatedObjectMeta.Fields {
			if relatedObjectField.Type == FieldTypeGeneric &&
				relatedObjectField.LinkType == LinkTypeOuter &&
				relatedObjectField.LinkMeta.Name == targetMeta.Name &&
				relatedObjectField.OuterLinkField.Name == genericInnerLinkFieldDescription.Name {
				//omit outer field and update related object
				delete(relatedObjectMeta.Fields, i)
				metaStore.Update(relatedObjectMeta.Name, relatedObjectMeta, false)
			}
		}
	}
}

//Remove inner fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedInnerLinks(targetMeta *Meta) {
	metaDescriptionList := metaStore.List()
	for _, objectMetaDescription := range metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name, false)
			if objectMeta != nil {
				objectNeedsUpdate := false

				for i, fieldDescription := range objectMeta.Fields {
					//omit orphan fields
					fieldIsTargetInnerLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeObject && fieldDescription.LinkMeta.Name == targetMeta.Name
					fieldIsTargetOuterLink := fieldDescription.LinkType == LinkTypeOuter && fieldDescription.Type == FieldTypeArray && fieldDescription.LinkMeta.Name == targetMeta.Name

					if fieldIsTargetInnerLink || fieldIsTargetOuterLink {
						delete(objectMeta.Fields, i)
					} else {
						objectNeedsUpdate = true
					}
				}
				// it means that related object should be updated
				if objectNeedsUpdate {
					metaStore.Update(objectMeta.Name, objectMeta, true)
				}
			}
		}
	}
}

//Remove inner fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedGenericInnerLinks(targetMeta *Meta) {
	metaDescriptionList := metaStore.List()
	for _, objectMetaDescription := range metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name, false)
			if objectMeta != nil {
				//objectMetaFields := make([]*Field, 0)
				objectNeedsUpdate := false

				for i, fieldDescription := range objectMeta.Fields {
					//omit orphan fields
					fieldIsTargetGenericInnerLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeGeneric && utils.Contains(fieldDescription.GetLinkMetaListNames(), targetMeta.Name)

					if fieldIsTargetGenericInnerLink {
						delete(objectMeta.Fields, i)
					} else {
						objectNeedsUpdate = true
						indexOfTargetMeta := utils.IndexOf(fieldDescription.GetLinkMetaListNames(), targetMeta.Name)

						//alter field
						field := objectMeta.Fields[i]
						field.LinkMetaList = append(field.LinkMetaList[:indexOfTargetMeta], field.LinkMetaList[indexOfTargetMeta+1:]...)
					}
				}
				// it means that related object should be updated
				if objectNeedsUpdate {
					metaStore.Update(objectMeta.Name, objectMeta, true)
				}
			}
		}
	}
}

//Remove inner fields linking to given MetaDescription
func (metaStore *MetaStore) removeRelatedObjectsFieldAndThroughMeta(keepMeta bool, targetMeta *Meta) error {
	metaDescriptionList := metaStore.List()
	for _, objectMetaDescription := range metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name, false)
			if objectMeta != nil {
				objectNeedsUpdate := false

				for i, fieldDescription := range objectMeta.Fields {
					//omit orphan fields
					fieldIsObjectsLink := fieldDescription.LinkType == LinkTypeInner && fieldDescription.Type == FieldTypeObjects

					if fieldIsObjectsLink {
						delete(objectMeta.Fields, i)
					} else {
						objectNeedsUpdate = true
						if !keepMeta {
							if _, err := metaStore.Remove(fieldDescription.LinkThrough.Name, true); err != nil {
								return err
							}
						}
					}
				}

				// means that related object should be updated
				if objectNeedsUpdate {
					if _, err := metaStore.Update(objectMeta.Name, objectMeta, true); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// compare current object`s version to the version is being updated and remove outer links
// if any inner link is being removed
func (metaStore *MetaStore) processInnerLinksRemoval(currentMeta *Meta, metaToBeUpdated *Meta) {
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
				metaStore.removeRelatedOuterLink(currentMeta, currentFieldDescription)
			}
		}
	}
}

// compare current object`s version to the version is being updated and remove outer links
// if any generic inner link is being removed
func (metaStore *MetaStore) processGenericInnerLinksRemoval(currentMeta *Meta, metaToBeUpdated *Meta) {
	for _, currentFieldDescription := range currentMeta.Fields {
		if currentFieldDescription.LinkType == LinkTypeInner && currentFieldDescription.Type == FieldTypeGeneric {
			fieldIsBeingRemoved := true
			fieldIsBeingUpdated := true
			var linkMetaListDiff []*Meta
			for _, fieldDescriptionToBeUpdated := range metaToBeUpdated.Fields {
				if fieldDescriptionToBeUpdated.Name == currentFieldDescription.Name &&
					fieldDescriptionToBeUpdated.LinkType == LinkTypeInner {
					if utils.Equal(fieldDescriptionToBeUpdated.GetLinkMetaListNames(), currentFieldDescription.GetLinkMetaListNames(), false) {
						fieldIsBeingRemoved = false
						fieldIsBeingUpdated = false
					} else {
						fieldIsBeingRemoved = false
						fieldIsBeingUpdated = true
						tmpCurrMetaList := MetaList{currentFieldDescription.LinkMetaList}
						linkMetaListDiff = tmpCurrMetaList.Diff(fieldDescriptionToBeUpdated.LinkMetaList)
					}
				}
			}
			//process generic outer link removal only for removed metas
			if fieldIsBeingUpdated {
				metaStore.removeRelatedToInnerGenericOuterLinks(currentMeta, currentFieldDescription, linkMetaListDiff)
			}
			//process generic outer link removal for each linked meta
			if fieldIsBeingRemoved {
				metaStore.removeRelatedToInnerGenericOuterLinks(currentMeta, currentFieldDescription, currentFieldDescription.LinkMetaList)
			}
		}
	}
}

//track outer link removal and return it if corresponding inner generic field was not removed
//do nothing if field is being renamed
func (metaStore *MetaStore) processGenericOuterLinkKeeping(previousMeta *Meta, currentMeta *Meta) {
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
func (metaStore *MetaStore) addReversedOuterGenericFields(previousMeta *Meta, currentMeta *Meta) {
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
						tmpPrevMetaList := MetaList{previousStateField.LinkMetaList}
						if excludedMetas = tmpPrevMetaList.Diff(field.LinkMetaList); len(excludedMetas) > 0 {
							shouldProcessOuterLinks = true
						}
						tmpMetaList := MetaList{field.LinkMetaList}
						if includedMetas = tmpMetaList.Diff(previousStateField.LinkMetaList); len(includedMetas) > 0 {
							shouldProcessOuterLinks = true
						}
					}
				} else {
					includedMetas = field.LinkMetaList
				}
			} else {
				includedMetas = field.LinkMetaList
			}

			if shouldProcessOuterLinks {
				//add reverse outer
				for _, linkMeta := range includedMetas {
					fieldName := ReverseInnerLinkName(currentMeta.Name)
					if linkMeta.FindField(fieldName) != nil {
						continue
					}
					outerField := &Field{
						Name:           fieldName,
						Type:           FieldTypeGeneric,
						LinkType:       LinkTypeOuter,
						LinkMeta:       currentMeta,
						OuterLinkField: field,
						Optional:       true,
						QueryMode:      true,
						RetrieveMode:   false,
					}
					linkMeta.AddField(outerField)
					metaStore.Update(linkMeta.Name, linkMeta, true)
				}

				//remove reverse outer
				for _, excludedMeta := range excludedMetas {
					for i, excludedField := range excludedMeta.Fields {
						if excludedField.Type == FieldTypeGeneric && excludedField.LinkType == LinkTypeOuter {
							if excludedField.OuterLinkField.Name == field.Name && excludedField.LinkMeta.Name == field.Meta.Name {
								delete(excludedMeta.Fields, i)
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
func (metaStore *MetaStore) addReversedOuterFields(previousMeta *Meta, currentMeta *Meta) {
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
				outerField := &Field{
					Name:           fieldName,
					Type:           FieldTypeArray,
					LinkType:       LinkTypeOuter,
					LinkMeta:       currentMeta,
					OuterLinkField: field,
					Optional:       true,
					QueryMode:      true,
					RetrieveMode:   false,
				}
				referencedMeta.AddField(outerField)
				metaStore.Update(referencedMeta.Name, referencedMeta, true)
			}
		}
	}
}

func (metaStore *MetaStore) Cache() *MetaCache {
	return metaStore.cache
}

//create through meta if it does not exist
func (metaStore *MetaStore) createThroughMeta(meta *Meta) error {
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
func (metaStore *MetaStore) Flush() error {
	for _, meta := range metaStore.List() {
		if _, err := metaStore.Remove(meta.Name, true); err != nil {
			return err
		}
	}
	return nil
}

func NewMetaStore(md MetaDescriptionSyncer, mds MetaDbSyncer, gtm *transactions.GlobalTransactionManager) *MetaStore {
	return &MetaStore{MetaDescriptionSyncer: md, Syncer: mds, cache: NewCache(), globalTransactionManager: gtm}
}
